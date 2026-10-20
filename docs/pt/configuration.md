# Referência de Configuração {: #configuration-reference }

O Provisa é configurado via um arquivo YAML (padrão: `config/provisa.yaml`).

## Includes

Divida uma config em vários arquivos com `includes:`. O arquivo que inclui lista caminhos de fragmento sob essa chave; o Provisa os mescla antes da validação, produzindo o mesmo resultado que escrever tudo em um único arquivo.

```yaml
# provisa-with-sources.yaml — wrapper that adds a Neo4j source to the base config
includes:
  - /path/to/config/provisa-install.yaml
  - /path/to/demo/sources/neo4j/fragment.yaml
```

Um arquivo wrapper contendo apenas `includes:` é válido. `load_control_plane` lê através dos includes, então a seção `control_plane:` é tomada de qualquer arquivo incluído que a defina.

**Regras de merge**

- Caminhos são resolvidos em relação ao arquivo que inclui. Caminhos absolutos são usados como estão.
- Seções de lista (`sources`, `tables`, `domains`, `relationships`, `roles`, …) são anexadas, e as entradas do fragmento seguem as entradas do arquivo que inclui.
- Uma chave escalar ou de mapeamento que o arquivo que inclui não define é tomada do fragmento.
- Uma chave que os dois arquivos definem com valores diferentes é um conflito; o carregamento falha, nomeando a chave.
- Valores idênticos em ambos os arquivos não são um conflito.
- Includes se aninham. Um arquivo que inclui a si mesmo — diretamente ou através de outro fragmento — é recusado.
- `includes` é consumido no momento do carregamento e nunca aparece na config validada.

## Fontes {: #sources }

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

Todas as fontes compartilham um conjunto comum de campos.

| Campo | Padrão | Notas |
| ------- | --------- | ------- |
| `id` | obrigatório | Alfanumérico, hífens, sublinhados |
| `type` | obrigatório | Ver tabela abaixo |
| `host` | `""` | Nome de host ou IP |
| `port` | `0` | `0` significa que cada conector fornece seu próprio padrão — não há um mapa central de portas padrão |
| `database` | `""` | |
| `username` | `""` | |
| `password` | `""` | Suporta referências de credencial `${env:VAR}` e `${secret:NAME}` — veja [Segredos](secrets.md) |
| `path` | `null` | Caminho de arquivo ou URI para fontes baseadas em arquivo |
| `base_url` | `null` | URL base para fontes de API |
| `pool_min` / `pool_max` | `1` / `5` | Limites do pool de conexão |
| `cache_enabled` | `true` | Alterna o cache para todas as tabelas desta fonte |
| `cache_ttl` | `null` | Segundos; `null` herda o padrão global; `-1` nunca expira |
| `federation_hints` | `{}` | Parâmetros estendidos por conector (dict[str,str]); ver referência de tipos abaixo. |
| `mapping` | `{}` | DSL de mapeamento para redis, elasticsearch, prometheus. |
| `allowed_domains` | `[]` | Restringe esta fonte a IDs de domínio específicos; vazio = irrestrito |
| `description` | `""` | |

### Tipos de fonte suportados {: #supported-source-types }

| Tipo | Estilo de conexão | Notas |
| ------ | ----------------- | ------- |
| **RDBMS** | | |
| `postgresql` | host/port | Pool asyncpg; PgBouncer opcional via `use_pgbouncer` |
| `mysql` | host/port | |
| `mariadb` | host/port | |
| `singlestore` | host/port | |
| `sqlserver` | host/port | |
| `oracle` | host/port | |
| `firebird` | host + `path` (arquivo do BD) | Extensão comunitária firebird do DuckDB |
| `duckdb` | host/port | |
| `cockroachdb` | host/port | Reaproveita o driver/dialeto PostgreSQL |
| `yugabytedb` | host/port | Reaproveita o driver/dialeto PostgreSQL |
| `greenplum` | host/port | Reaproveita o driver/dialeto PostgreSQL |
| `tidb` | host/port | Reaproveita o driver/dialeto MySQL |
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
| `hudi` | `path` (URI da tabela) | Motor Hudi do ClickHouse, zero-copy |
| `hive` | host/port (metastore) + `mapping.storage` | Backend de armazenamento em `mapping["storage"]`: hadoop/hdfs/local/s3/azure/adls |
| `hive_s3` | host/port (metastore) + chaves S3 em `mapping` | Tipo distinto; sempre armazenamento S3 |
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
| `airport` | `base_url` (localização Flight) | Extensão airport do DuckDB |
| `ingest` | receptor push | Serviços externos enviam POST de eventos JSON |
| **SaaS** | | |
| `sharepoint` | `base_url` ou `host` + `mapping` | Autenticação via `mapping.auth_type` |
| `splunk` | `host`/`port` ou `base_url` + `mapping` | |
| **GovData** | | |
| `govdata` | subject + `domain_id` | Modelo separado `GovDataSource`; ver §GovData abaixo |
| **Qualidade de Dados** | | |
| `soda` | host/port apontado para o pgwire do Provisa | Requer o extra `soda`; Elastic License 2.0, somente self-hosted |
| `great_expectations` | host/port apontado para o pgwire do Provisa | Requer o extra `gx`; Apache 2.0 |

### Referência de tipos de fonte {: #source-type-reference }

Cada tipo que precisa de configuração não óbvia tem uma entrada curta abaixo. Os tipos RDBMS (postgresql, mysql etc.) usam só os campos comuns acima e não precisam de seção própria.

#### GovData

Fontes `govdata` usam um modelo de nível superior próprio, `GovDataSource`, em vez do `Source` genérico. O acesso é dividido por agrupamento de assuntos.

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

Cada subject mapeia para um ou mais esquemas GovData. Configurar uma fonte `govdata` com um subject expõe todos os esquemas para esse subject automaticamente.

| Subject | Esquemas |
| --------- | --------- |
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

Os esquemas `ref` e `geo` sempre entram como esquemas de ligação. Eles não são configuráveis e não aparecem na lista acima. Use o assunto `ALL` para dar acesso a todos os esquemas.

#### Kafka

A linha `kafka` em `sources:` só registra a fonte. O `details()` do conector dela devolve `{}`; a configuração de verdade fica no bloco de nível superior `kafka_sources[]`. O Kafka é sempre um VIRTUAL_SOURCE: passa pelo motor e não tem pool direto.

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

**Janela de tempo** — `default_window` limita toda consulta a um período recente, evitando leituras ilimitadas de tópicos de alto volume. Formato: `1h`, `30m`, `7d`, `60s`. Padrão `1h`. Auto-injetado como `WHERE _timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`. Clientes podem sobrepor com seu próprio filtro `_timestamp` no argumento `where` do GraphQL.

**Discriminador** — Múltiplas configurações de tópico podem apontar para o mesmo tópico Kafka físico com valores de `discriminator` diferentes, produzindo tipos GraphQL separados. O discriminador é auto-injetado como uma cláusula WHERE.

**Origem do esquema**

| Valor | Comportamento |
| ------- | ------- |
| `registry` | Busca o esquema no Confluent Schema Registry |
| `manual` | Define colunas inline na configuração (sem necessidade de Schema Registry) |
| `sample` | Descobre automaticamente a partir de mensagens de amostra |

#### Snowflake

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

#### Databricks

`http_path` em `federation_hints` é obrigatório. `password` leva o token de acesso pessoal. `catalog` é opcional e vai no SQL ou nos hints; o campo `database` não o guarda.

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

#### BigQuery

`project` em `federation_hints` é obrigatório. A autenticação usa `GOOGLE_APPLICATION_CREDENTIALS` (caminho para um arquivo de chave de service account) ou Application Default Credentials no ambiente do motor.

```yaml
sources:
  - id: my-bigquery
    type: bigquery
    federation_hints:
      project: my-gcp-project     # required
```

#### Fabric / Synapse

Os dois usam T-SQL sobre TDS com autenticação do Azure AD. Entre com `az login` (desenvolvimento) ou com uma identidade gerenciada (produção); o motor lê as credenciais pelo `DefaultAzureCredential` do `azure-identity`. Os dados de conexão vêm de variáveis de ambiente, `FABRIC_SQL_SERVER` / `FABRIC_DATABASE` (Fabric) ou `SYNAPSE_SQL_SERVER` / `SYNAPSE_DATABASE` (Synapse), ou de `PROVISA_ENGINE_URL`.

```yaml
sources:
  - id: my-fabric
    type: fabric
    # host/database read from FABRIC_SQL_SERVER / FABRIC_DATABASE when not set here
```

#### ClickHouse

`secure` em `federation_hints` habilita TLS na interface HTTP. A porta padrão é `8123` (simples) ou `8443` (quando `secure: "true"`). `schema` em `federation_hints` sobrepõe o esquema remoto.

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

#### Delta Lake / Iceberg

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

#### Hive / Hive S3

`host` e `port` apontam para o metastore Thrift do Hive (porta padrão 9083). Para `hive`, defina `mapping["storage"]` para escolher o backend do armazenamento de objetos. Uma chave obrigatória ausente falha de forma visível, sem valor reserva.

`hive_s3` é um tipo distinto que sempre declara armazenamento S3; não precisa de `mapping.storage`.

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

#### Redis

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
          key_pattern: "sessions:*"
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

#### Elasticsearch

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

#### Prometheus

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

#### Google Sheets

`spreadsheet_id` em `federation_hints` é obrigatório. A autenticação usa um SECRET `gsheet` do DuckDB provisionado no momento do attach.

```yaml
sources:
  - id: my-sheet
    type: google_sheets
    federation_hints:
      spreadsheet_id: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
```

#### Fontes de arquivo (csv / parquet / sqlite / files) {: #file-sources-csv-parquet-sqlite-files }

`path` é obrigatório. `files` percorre um diretório em busca de arquivos CSV, Parquet, XLSX e JSON, expondo cada um como uma tabela. Todas as fontes baseadas em arquivo são VIRTUAL (roteiam pelo motor; sem pool direto).

```yaml
sources:
  - id: orders-csv
    type: csv
    path: /data/orders.csv

  - id: data-lake-dir
    type: files
    path: /data/lake/         # directory; each file becomes a table
```

**Conjuntos de dados Kaggle** não podem ser adicionados por este arquivo, porque precisam de um token válido e do seletor de conjuntos de dados do formulário de fontes (Fontes → Subscriptions → Kaggle). Uma fonte Kaggle exportada para YAML aparece como `type: files` com `kaggle_owner` e `kaggle_ref` em `federation_hints`. Baixar de novo do Kaggle exige a mutation `refreshKaggleSource` ou a atualização na UI; editar o YAML não faz isso. Veja [Conjuntos de dados Kaggle](sources.md#kaggle-datasets) na referência de tipos de fonte.

#### Fontes de API / Remotas {: #api-remote-sources }

**openapi** — defina `base_url` para a URL base do OpenAPI. A descoberta de esquema lê a especificação OpenAPI na inicialização.

```yaml
sources:
  - id: payment-api
    type: openapi
    base_url: https://api.payments.example.com/v1
```

**graphql_remote** — defina `base_url`. Chaves opcionais de `mapping`: `headers` (dict de cabeçalhos estáticos), `forward_client_headers` (bool), `timeout_seconds` (int).

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

**airport** — `base_url` é a localização do servidor Arrow Flight. Extensão airport do DuckDB.

```yaml
sources:
  - id: flight-source
    type: airport
    base_url: grpc://flight.internal:8815
```

**websocket / rss** — use `host`, `port`, `path` e `federation_hints`.

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

**sharepoint**

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

**splunk**

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

#### Verificadores de qualidade de dados (soda / great_expectations) {: #data-quality-checkers-soda-great_expectations }

Uma fonte verificadora aponta para o próprio endpoint pgwire do Provisa, então um único driver postgres varre a visão federada de uma tabela apoiada em Snowflake ou Iceberg. Você declara a identidade da varredura; ela nunca é herdada. A política se aplica a essa conexão, e um conjunto filtrado de linhas não pode deixar uma verificação passar em silêncio. As chaves de conexão vêm de `mapping`: `host`, `port`, `database`, `user`, `password`.

```yaml
sources:
  - id: dq
    type: soda                 # or great_expectations
    domain_id: sales-analytics
    mapping:
      host: localhost
      port: 5439               # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner
      password: ${env:PROVISA_DQ_PASSWORD}
```

Cada tabela de resultados traz `dq_contract`, o YAML do contrato Soda ou o JSON da suíte Great Expectations palavra por palavra. Colunas, marca d'água e promoções derivam dele; veja como em [Verificadores de Qualidade de Dados](sources.md#data-quality-checkers).

**Escolha na instalação.** O verificador não é vinculado: a varredura roda num interpretador filho, e a biblioteca só é instalada quando um operador a nomeia. Todo caminho de instalação (`install.sh`, `packaging/linux/first-launch.sh` e o assistente do macOS via `PROVISA_DQ_CHECKER`) grava a escolha em `~/.provisa/config.yaml`:

```yaml
dq_checker: none        # none | soda | gx
```

`scripts/provisa` lê essa chave e exporta `PROVISA_EXTRAS`, que o `docker-compose.app.yml` passa como argumento de build ao `ARG PROVISA_EXTRAS` do `Dockerfile`:

| `dq_checker` | `PROVISA_EXTRAS` (camada Docker) | Instalação venv nativa |
| -------------- | -------------------------------- | --------------------- |
| `none` | `firebase,vector` | `provisa[embedded]` |
| `soda` | `firebase,vector,soda` | `provisa[embedded,soda]` |
| `gx` | `firebase,vector,gx` | `provisa[embedded,gx]` |

Instalar o conjunto de dados demo eleva `none` para `gx` e informa isso, porque a configuração demo registra uma suíte Great Expectations sobre `pet_store.pets`, e seu scorecard de qualidade não teria nada para mostrar caso contrário. Nomear `soda` mantém `soda`.

Se você chega à demo pelo pip em vez de um instalador, pula esse passo do assistente, então o extra `demo` traz o mesmo verificador: `provisa run --demo` precisa de `pip install 'provisa[embedded,demo]'` para a varredura rodar. Sem ele, a varredura informa `data-quality checker 'great_expectations' is not installed` e mostra o comando de instalação.

Qualquer outro valor para o inicializador, que assim nunca sobe sem o verificador que o operador pediu. O extra `soda` traz `soda-postgres`, e `gx` traz `great-expectations[postgresql]`. O Soda Core está sob a Elastic License 2.0: `config/capabilities.yaml` marca a opção como `cloud_eligible: false`, e o plano hospedado a recusa.

## Domínios {: #domains }

```yaml
domains:
  - id: sales-analytics
    description: Sales operational data
```

## Nomenclatura {: #naming }

```yaml
naming:
  convention: apollo_graphql   # snake, hasura_graphql, apollo_graphql (default)
  domain_prefix: true          # prepend domain_id__ to all GraphQL names
  rules:
    - pattern: "^prod_pg_"
      replace: ""
```

### Convenção de nomenclatura {: #naming-convention }

A autoridade de nomenclatura é a única fonte de verdade para nomes voltados ao cliente; nomes físicos de colunas do backend nunca são expostos aos clientes. Cada linguagem de consulta deriva o nome de uma coluna a partir de seu `column.alias`, se definido, senão a partir do nome físico da coluna via sua convenção configurada.

A convenção GraphQL é uma de três enums predefinidos. Strings de formato livre antigas (`none`, `snake_case`, `camelCase`, `PascalCase`) estão descontinuadas.

| Preset | Padrão | Nomes de tipo | Nomes de campo | Nomes de mutação |
| -------- | --------- | ------------ | ------------- | ---------------- |
| `apollo_graphql` | sim | PascalCase | camelCase | camelCase |
| `hasura_graphql` | | PascalCase | camelCase | snake_case |
| `snake` | | PascalCase | snake_case | snake_case |

A convenção GraphQL padrão é `apollo_graphql`, que produz nomes de campo e mutação em camelCase. A convenção SQL é separada, com padrão `snake_case`, aplicada via `apply_sql_name()`; a convenção GraphQL é aplicada via `apply_gql_name()`, e o nome CQL é derivado do nome GraphQL.

`domain_prefix: bool` é uma opção ortogonal que se aplica independentemente do preset escolhido.

Um `column.alias` explícito é o nome canônico: SQL o usa literalmente sem aplicar convenção, GraphQL aplica sua convenção a ele, e CQL deriva do nome GraphQL.

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

### Prefixo de domínio {: #domain-prefix }

Quando `domain_prefix: true`, todos os nomes de campo e tipo GraphQL recebem o prefixo do ID de domínio usando um separador de sublinhado duplo:

| Tabela | Domínio | Nome do campo |
| ------- | -------- | ----------- |
| `orders` | `sales-analytics` | `sales_analytics__orders` |
| `customer_segments` | `customer-insights` | `customer_insights__customer_segments` |

Isso evita colisões de nome quando domínios diferentes têm tabelas com o mesmo nome, e torna as consultas autoexplicativas.

### Regras de nomenclatura {: #naming-rules }

Regras de regex aplicadas a nomes de tabela ao gerar nomes de campo GraphQL. Aplicadas em ordem antes da resolução de unicidade.

## Tabelas {: #tables }

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
      - name: email
        visible_to: [admin, analyst]
        masks:                    # first entry the reader holds applies
          - to: [admin]           # admin sees raw
          - to: ["*"]             # everyone else sees it masked
            mask: {type: regex, pattern: "^(.{2}).*(@.*)$", replace: "$1***$2"}
        alias: email_address      # optional: override GraphQL field name
        description: "Primary email address"  # optional: appears in SDL
      - name: amount
        visible_to: [admin]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: constant, value: 0}
      - name: created_by
        visible_to: [admin, analyst]
      - name: created_at
        visible_to: [admin, analyst]
        masks:
          - to: [admin]
          - to: ["*"]
            mask: {type: truncate, precision: month}
    write_grants:                 # who may write this table, per role and operation
      - role: admin
        operation: insert
        columns: [email, amount]
        check: "TRUE"             # any row may be inserted
        presets:                  # set by the grant; the statement may not supply them
          - {column: created_by, source: session, name: user_id}
          - {column: created_at, source: now}
      - role: admin
        operation: update
        columns: [email, amount]
        rows: "TRUE"              # any row may be updated
        check: "TRUE"
```

Por padrão, uma tabela não tem nenhuma permissão de escrita, então nenhuma função pode escrever nela. Uma permissão dá a uma função uma operação (`insert`, `update` ou `delete`) sobre a tabela. Uma tabela contém no máximo uma permissão por função e operação.

| Campo da permissão | Significado |
| ----------- | ------- |
| `columns` | As colunas que um `INSERT` pode listar ou um `UPDATE` pode definir. Uma permissão `delete` não nomeia nenhuma. |
| `rows` | As linhas que um `UPDATE` ou `DELETE` pode tocar: um predicado SQL sobre as colunas da tabela. Uma permissão `insert` não tem. |
| `check` | O que toda linha escrita por um `INSERT`, ou deixada por um `UPDATE`, deve satisfazer: um predicado SQL. Uma permissão `delete` não tem. |
| `presets` | Colunas que a própria permissão define. A instrução não pode fornecê-las, então uma coluna com preset não está também em `columns`. |

Se você omitir `rows` ou `check`, a permissão assume o filtro de linhas de leitura da função sobre a tabela: uma função escreve as linhas que lê, a menos que a permissão diga outra coisa. `"TRUE"` significa todas as linhas. As chaves antigas `writable_by` da coluna e `column_presets` da tabela são recusadas pelo nome; a configuração não carrega até que sejam substituídas por `write_grants`.

Toda escrita em toda superfície é admitida nesta ordem: a função tem o direito `write`; a função tem uma permissão para a operação sobre a tabela; cada coluna que a instrução escreve está na permissão; os presets da permissão são adicionados à instrução; o predicado `rows` é combinado com AND no `WHERE` de um `UPDATE` ou `DELETE`; e `check` é avaliado sobre as novas linhas antes de qualquer coisa ser enviada à fonte. Uma instrução que `check` não consegue decidir é recusada. As colunas da permissão também decidem quais campos de mutação o GraphQL oferece a uma função: um campo por operação para a qual ela tem uma permissão, com exatamente essas colunas.

Algumas instruções exigem uma permissão mais ampla. `TRUNCATE` exige uma permissão `delete` sobre todas as linhas. `MERGE` exige uma permissão `insert` e uma `update`, ambas sobre todas as linhas. `INSERT ... ON CONFLICT DO UPDATE` exige uma permissão `update` sobre todas as linhas sem presets. `COPY ... FROM STDIN` exige uma permissão `insert` sem presets.

As permissões são herdadas. Uma função filha assume, por tabela e operação, a permissão da função mais próxima em sua cadeia que tenha uma, a sua própria primeiro. Uma meta-função mantém a dobra das permissões de seus membros: a união de suas colunas, o OR de seus predicados `rows` e `check`, e apenas os presets que todos os membros definem igualmente.

Na interface de administração, a seção **Gravações** de uma tabela lista as permissões e as edita em conjunto.

### Aliases

Aliases de tabela e coluna sobrepõem o nome GraphQL padrão. Úteis para:

- Renomear nomes de banco de dados crípticos (ex.: `tbl_cust_seg` → `customer_segments`)
- Evitar abreviações na camada de API
- Criar um vocabulário limpo e específico do domínio

### Descrições {: #descriptions }

Descrições de tabela e coluna são incluídas no SDL GraphQL gerado. Elas aparecem no explorador de documentação do GraphiQL e em consultas de introspecção. Defina-as no YAML de configuração ou via a UI de administração.

### Path (Extração JSON computada) {: #path-computed-json-extraction }

Colunas podem extrair valores de uma coluna de origem JSON/JSONB usando um `path` em notação de ponto. Útil para dados semiestruturados em mensagens Kafka, documentos MongoDB ou colunas JSONB do PostgreSQL.

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

O formato do path é `source_column.key1.key2...`. O compilador gera `json_extract_scalar(source_column, '$.key1.key2')` no SQL.

**Impacto no roteamento:** Colunas com path usam operadores JSON do PostgreSQL (`->>`), suportados nativamente pelo roteamento direto ao PG. Para fontes não-PostgreSQL (MySQL, SQL Server, etc.), consultas com colunas de path são automaticamente roteadas pelo motor de federação. Mutações não são afetadas, já que colunas de path são campos computados somente-leitura.

### Tipos de mascaramento {: #masking-types }

| Tipo | Campos | Descrição |
| ------ | -------- | ------------- |
| `regex` | `pattern`, `replace` | REGEXP_REPLACE (apenas colunas string) |
| `constant` | `value` | Substituição literal (NULL, 0, MAX, MIN, personalizado) |
| `truncate` | `precision` | DATE_TRUNC (apenas colunas date/timestamp) |

## Relacionamentos {: #relationships }

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

### Auto-materialização {: #auto-materialization }

Defina `materialize: true` em um relacionamento para gerar automaticamente uma view materializada para JOINs entre fontes. Isso evita consultas federadas custosas pré-computando o resultado do JOIN.

- Apenas relacionamentos entre fontes geram MVs (JOINs dentro da mesma fonte já são rápidos)
- Em um relacionamento apoiado em junção, a MV cobre a travessia de dois saltos: salto de origem, salto da junção, discriminador e as colunas próprias da junção como atributos da aresta. A junção conta como uma perna, então uma aresta é entre fontes quando qualquer uma das três tabelas fica em uma fonte diferente
- A MV começa desatualizada e é populada pelo laço de atualização em segundo plano
- Mutações em qualquer uma das tabelas de origem marcam a MV como desatualizada para nova atualização
- `refresh_interval` tem padrão de 300 segundos (5 minutos)

## Funções {: #roles }

```yaml
roles:
  - id: admin
    description: Administers sources, tables, relationships and security.
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
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: [sales-analytics]
    parent_role_id: analyst      # inherits query_development + sales-analytics
```

Funções com `parent_role_id` herdam capacidades, acesso a domínio, concessões de coluna e objeto, e regras de RLS do pai, com a própria regra de RLS da filha para uma tabela tendo precedência. A cadeia é achatada na inicialização.

### Capacidades {: #capabilities }

| Capacidade | Descrição |
| ----------- | ------------- |
| `source_registration` | Registrar fontes de dados |
| `table_registration` | Registrar tabelas |
| `relationship_registration` | Definir relacionamentos |
| `security_config` | Configurar RLS, mascaramento |
| `query_development` | Executar consultas |
| `full_results` | Ignorar limites de amostragem |
| `admin` | Todas as capacidades |

## Regras de RLS {: #rls-rules }

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

## Views Materializadas {: #materialized-views }

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
      # Add via_table with via_left_column/via_right_column (and
      # via_type_column/via_type_value when the junction is discriminated) to
      # cover a two-hop junction traversal instead of a direct join.
    target_catalog: postgresql
    target_schema: mv_cache
    refresh_interval: 300
    enabled: true
```

## Views (conjuntos de dados computados governados) {: #views-governed-computed-datasets }

Views são conjuntos de dados computados definidos por SQL com governança completa em nível de coluna. São o mecanismo governado para adicionar agregações, transformações e métricas derivadas ao modelo.

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
| ------- | ---------- | ------------- |
| `id` | Sim | Identificador único da view |
| `sql` | Sim | Instrução SQL SELECT que define a view |
| `domain_id` | Sim | Domínio para visibilidade de esquema |
| `materialize` | Não | `true` = atualização periódica via CTAS, `false` = view federada ao vivo |
| `refresh_interval` | Não | Segundos entre atualizações (apenas materializadas, padrão 300) |
| `description` | Não | Aparece no SDL GraphQL |
| `alias` | Não | Sobrepõe o nome GraphQL |
| `columns` | Sim | Definições de coluna com visibilidade, mascaramento, descrições |

### Materializada vs ao vivo {: #materialized-vs-live }

- **`materialize: true`**: O Provisa cria uma tabela via CTAS e a atualiza em uma programação. Consultas mais rápidas, mas os dados podem estar desatualizados em até `refresh_interval` segundos.
- **`materialize: false`**: O Provisa cria uma view federada. Consultas sempre retornam dados ao vivo, mas podem ser mais lentas para agregações complexas.

Views passam pelo mesmo pipeline de governança que as tabelas: RLS, mascaramento, amostragem e visibilidade por papel. Assim nada pode acrescentar semântica nova à plataforma sem a supervisão de um steward.

### Views com histórico (viagem no tempo) {: #views-with-history-time-travel }

Uma tabela ou uma view materializada pode guardar cada versão das suas linhas e ser lida num momento passado. Para isso você mapeia três colunas da fonte, e declara as três ou nenhuma:

```yaml
views:
  - id: customer-history
    sql: SELECT customer_id, tier, effective_date, updated_at FROM crm.customers
    domain_id: sales
    materialize: true
    business_key: [customer_id]
    valid_time: effective_date
    recorded_time: updated_at
    columns:
      - {name: customer_id, visible_to: [analyst]}
      - {name: tier, visible_to: [analyst]}
      - {name: effective_date, visible_to: [analyst]}
      - {name: updated_at, visible_to: [analyst]}
```

| Campo | Descrição |
| ------- | ------------- |
| `business_key` | As colunas que identificam a mesma coisa entre versões, como `transaction_id`. Todas as versões de um registro compartilham esta chave. |
| `valid_time` | A coluna com a data que o seu negócio dá ao registro: quando foi acordado, lançado ou entrou em vigor. |
| `recorded_time` | A coluna com o momento em que esta versão do registro foi inserida ou alterada, como `updated_at`. |

Cada uma nomeia uma coluna da tabela ou view. Uma view que as declara define `materialize: true`, e o Provisa escolhe como guarda as versões. Uma entrada em `tables:` aceita as mesmas três chaves.

Os dois tempos têm outros nomes: o tempo de registro (recorded time), chamado tempo de transação na literatura de bancos de dados temporais e tempo de sistema no SQL:2011; o tempo válido (valid time), chamado tempo de aplicação no SQL:2011 ou data efetiva.

Os dois podem diferir para a mesma versão:

| Exemplo | `valid_time` | `recorded_time` |
| ------- | ------------ | --------------- |
| Um pagamento de 100 de 3 de outubro é corrigido para 120 em 5 de outubro. | A correção mantém 3 de outubro. | A correção recebe 5 de outubro. |
| Uma medição de campo feita em 3 de outubro às 14:10 é digitada em 6 de outubro. Um erro de digitação nela é corrigido em 7 de outubro. | 3 de outubro 14:10, mantido pela correção. | 6 de outubro. A correção recebe 7 de outubro. |
| Uma operação é lançada na execução com data de operação 3 de outubro. Em 4 de outubro a quantidade se revela errada e é alterada. | Data de operação 3 de outubro, mantida pela alteração. | 3 de outubro 10:02. A alteração recebe 4 de outubro. |

Lida como registrada em 3 de outubro, a operação mostra a quantidade errada. Lida como se sabe agora, mostra a corrigida.

Uma versão vigora do seu momento até o da versão seguinte. Uma exclusão tem efeito quando o Provisa a registra.

**Leitura.** Uma leitura simples responde a versão de cada chave vigente agora. O cabeçalho `X-Provisa-As-Of` lê em um momento, ou sobre um intervalo dado como intervalo ISO 8601 `start/end`:

```
X-Provisa-As-Of: 2026-03-01T00:00:00
X-Provisa-As-Of: 2026-01-01T00:00:00/2026-04-01T00:00:00
```

Um momento responde a versão de cada chave vigente então. Um intervalo responde cada versão vigente em algum momento dentro dele, uma linha por versão.

**Como registrado.** Uma correção registrada depois muda o que uma visão diz sobre o passado: a versão em vigor em 30 de setembro passa a ser a corrigida assim que a correção é registrada. Para ler a visão como o Provisa a tinha registrado num momento, antes de qualquer correção posterior, adicione `X-Provisa-As-Recorded`, um instante ISO 8601:

```
X-Provisa-As-Of: 2026-09-30T12:00:00
X-Provisa-As-Recorded: 2026-10-01T00:00:00
```

Toda versão registrada depois desse momento fica de fora, e o as-of então escolhe a versão em vigor no seu próprio momento. Com `X-Provisa-As-Recorded` sozinho, a leitura responde com a versão de cada chave em vigor conforme registrado naquele momento. Ele aceita um instante, não um intervalo; um intervalo ou um valor malformado é recusado com HTTP 400 (`data.invalid_as_recorded`).

Os dois instantes também chegam ao Provisa sem os cabeçalhos. O instante as-of é o argumento `as_of` de `resolve` (GraphQL, gRPC, MCP), o parâmetro de consulta REST `as_of` e a opção `as_of` de um ticket do Arrow Flight, e um intervalo passado a `resolve` responde com as `versions` da linha no lugar da sua `row`. O instante as-recorded é o argumento `as_recorded` de `resolve`, o parâmetro de consulta REST `as_recorded` e a opção `as_recorded` de um ticket do Arrow Flight. O registro de auditoria de cada instrução guarda os dois instantes com que leu, `as_of` e `as_recorded`. Direitos, máscaras e regras de linha se aplicam como em qualquer leitura. Um valor as-of malformado, ou um intervalo cujo fim não vem depois do início, é recusado com HTTP 400 (`data.invalid_as_of`).

Toda view com histórico responde mais três colunas, concedidas como a sua coluna de hora de origem (senão a sua primeira coluna de chave):

| Coluna | Significado |
| ------- | ------- |
| `source_from` | Quando esta versão passou a valer: sua hora de origem, ou sua hora de carga na falta dela |
| `source_to` | Quando a versão seguinte passou a valer; vazia enquanto esta vigora |
| `recorded_at` | Quando o Provisa registrou esta versão |

**Hora de origem a partir da captura de dados de mudança.** Uma tabela pousada por captura de dados de mudança traz sua hora de commit como `source_committed_at`, para que uma view com histórico a nomeie como hora de origem:

- Debezium: a hora de commit do banco de dados que cada evento de mudança traz (`source.ts_ms`). Uma linha copiada por uma construção completa da réplica não tem evento de mudança, então seu `source_committed_at` fica vazio até a linha mudar de novo, a menos que a fonte seja PostgreSQL com os timestamps de commit ativados (próximo item).
- PostgreSQL com o sinal de mudança `native`: a hora de commit que o PostgreSQL guarda para cada linha (`pg_xact_commit_timestamp(xmin)`), lida pelo driver próprio da fonte quando a réplica é construída. O PostgreSQL só a guarda com `track_commit_timestamp = on`. Ative-o para ter a hora de origem com o sinal nativo; sem ele, `source_committed_at` fica vazio. Ativar ou desativar a configuração reconstrói a réplica.

Tópicos Kafka sem o envelope do Debezium não trazem hora de commit.

### Views em um calendário {: #views-on-a-calendar }

Uma view materializada pode tirar seu agendamento de um calendário: ela é construída no fechamento de cada janela que o calendário lhe dá (um dia útil, uma semana, um mês, um trimestre ou um ano), e uma view que guarda histórico marca cada versão com esse fechamento. Uma configuração declara seus calendários em `calendars:`:

```yaml
calendars:
  - name: bank
    version: v1
    tz: America/New_York
    day_close: "18:00"
    holidays: [2026-11-26, 2026-12-25]

views:
  - id: daily-totals
    sql: |
      SELECT store, business_date, SUM(amount) AS total
      FROM fin.payments
      GROUP BY store, business_date
    domain_id: finance
    materialize: true
    calendar: bank
    grain: daily
    business_day_grain: true
    columns:
      - {name: store, visible_to: [analyst]}
      - {name: business_date, visible_to: [analyst]}
      - {name: total, visible_to: [analyst]}
```

| Campo | Obrigatório | Descrição |
| ------- | ---------- | ------------- |
| `name` | Sim | O nome que uma view informa como seu `calendar` |
| `version` | Sim | O rótulo da versão. Um nome pode ter várias versões, cada uma com seus próprios feriados. |
| `base_system` | Não | `gregorian` (padrão), `fiscal` ou `retail_445` |
| `tz` | Não | O fuso horário IANA em que estão os dias do calendário (padrão `UTC`) |
| `fiscal_anchor_month`, `fiscal_anchor_day` | Não | O mês e o dia em que começa um ano fiscal (padrão 1 e 1) |
| `retail_anchor` | Não | A primeira data de um ano 4-4-5 de referência; `retail_445` precisa dela |
| `week_start` | Não | O dia em que começa uma semana, 0 = segunda a 6 = domingo (padrão 0) |
| `holidays` | Não | Datas que não são dias úteis |
| `weekend` | Não | Os dias da semana que não são úteis, 0 = segunda (padrão `[5, 6]`) |
| `day_close` | Não | A hora local em que um dia útil fecha, `"HH:MM"`. O dia D vai então do fechamento do dia anterior ao fechamento de D. Sem ela, um dia termina à meia-noite. Coloque-a entre aspas: o YAML lê um `18:00` sem aspas como número. |

Um calendário é armazenado antes das tabelas, para que as views que o nomeiam o encontrem. Uma entrada que a mutation `createCalendar` recusaria é recusada com a mesma mensagem `invalid calendar: ...`, e a configuração não é carregada. A exportação do modelo escreve cada calendário em `calendars:`.

Uma view nomeia seu calendário com estas chaves. Uma entrada em `tables:` as recebe como `mv_calendar`, `mv_grain`, `mv_business_day_grain`, `mv_allowed_lateness` e `mv_expected_events`.

| Campo | Descrição |
| ------- | ------------- |
| `calendar` | O `name` do calendário |
| `grain` | `daily`, `weekly`, `monthly`, `quarterly` ou `annual`; um enésimo dia da semana como `3WE` (terceira quarta-feira) ou `LFR` (última sexta-feira); ou uma regra RFC 5545 como `FREQ=WEEKLY;INTERVAL=2;BYDAY=MO` |
| `business_day_grain` | `true`: uma janela diária só existe em dia útil, então nenhuma construção é executada em fim de semana ou feriado |
| `allowed_lateness` | Segundos após o fechamento de uma janela em que a construção espera pela entrada confirmada com atraso (padrão 0) |
| `expected_events` | As entradas que precisam estar atualizadas até o fechamento da janela. Se omitido, cada entrada que a view lê; `[]`, nenhuma. |

Uma view que nomeia um calendário que nenhuma configuração nem `createCalendar` declarou é recusada pelo nome quando o loop de eventos inicia: `unknown calendar 'ledger'; registered: [...]`.

**Construir um dia útil.** O argumento `window` da mutation `refreshMv`, uma data, constrói a view selada no fechamento desse dia útil:

```graphql
mutation {
  refreshMv(mvId: "daily-totals", window: "2026-10-02") { success message code }
}
```

O que a construção acrescenta é marcado com o fechamento da janela do calendário para essa data, como a própria construção no fechamento o marcaria, de modo que uma leitura conforme registrado nesse fechamento o devolve. Uma view com histórico construída assim reconstrói as datas cuja entrada mudou desde sua última construção, de modo que uma correção registrada antes da construção do dia seguinte é captada para a sua própria data. Uma view que não nomeia nenhum calendário é recusada (`schema.mv_window_needs_calendar`), assim como uma data em que seu calendário não tem janela, como um feriado com granularidade de dias úteis.

### Views somente-consulta {: #query-only-views }

Views com `materialize: true` e `materialize: false` expõem seu tipo GraphQL como somente-consulta. Nenhuma mutação de insert, upsert, update ou delete é gerada para relações apoiadas em `view_sql`.

## Cache

```yaml
cache:
  enabled: true
  redis_url: ${env:REDIS_URL}
  default_ttl: 300
```

### Hierarquia de cache {: #cache-hierarchy }

Ordem de resolução de TTL (mais específico vence): **tabela** > **fonte** > **padrão global**. O primeiro valor não nulo é usado.

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

Definir `cache_enabled: false` em uma fonte desabilita o cache para todas as tabelas dessa fonte, independentemente do TTL em nível de tabela. As chaves de cache sempre incluem `role_id` + valores de contexto RLS para particionamento de segurança.

### Adesão por requisição {: #per-request-opt-in }

O cache de respostas fica desligado para toda requisição que não o pede. Uma requisição adere com:

| Superfície | Adesão |
| --- | --- |
| GraphQL (`/data/graphql`, Arrow Flight) | `@cached` na operação, opcionalmente `@cached(ttl: 60)` |
| SQL (pgwire, Flight SQL, `/data/sql`) | uma linha de comentário `-- @provisa cache=true` ou `-- @provisa cache_ttl=60` na instrução |
| Cypher (`/data/cypher`, a Neo4j Query API, Bolt, Arrow Flight) | uma linha de comentário `// @provisa cache=true` ou `// @provisa cache_ttl=60` na consulta |
| gRPC (e o proxy gRPC HTTP) | metadados da chamada (cabeçalhos no proxy) `x-provisa-cache: true` ou `x-provisa-cache-ttl: 60` |
| REST, JSON:API e, como alternativa à indicação no texto: GraphQL sobre HTTP, `/data/cypher`, a Neo4j Query API | o cabeçalho de requisição `Cache-Control: max-age=60` (ou `max-stale=60`), ou `x-provisa-cache: true` / `x-provisa-cache-ttl: 60` |

Um `ttl` que não seja inteiro faz a requisição falhar. Uma requisição que indica a adesão duas vezes (no texto e em um cabeçalho, ou em dois cabeçalhos) com valores diferentes falha com `400`; nenhum é preferido. `Cache-Control: no-cache` ou `no-store` significa sem cache, o mesmo que sem cabeçalho.

Sem a indicação, a requisição não lê nem grava o cache. Com ela, as configurações acima continuam decidindo: o resultado só é armazenado se cada fonte lida tiver `cache_enabled: true` e o TTL resolvido de cada tabela for maior que 0. A entrada dura o `ttl` da requisição, se ela informar um; caso contrário, o menor TTL resolvido das tabelas lidas.

A indicação apenas aceita dados mais antigos em troca de velocidade. Ela nunca torna uma leitura mais recente do que as configurações do operador permitem: o frescor do carregamento e da réplica (`cache_ttl`, `change_signal`), `row_materialize`, os snapshots `load_protected` e as verificações de frescor valem para uma requisição com indicação exatamente como para uma sem. Veja [Frescor e carga: quem decide](#freshness-and-load-who-decides).

`@noCache` e `-- @provisa no_cache=true` não existem mais: com o cache desligado por padrão, não há o que contornar.

Uma gravação invalida toda entrada de cache que a organização que grava mantém para as tabelas gravadas; as entradas de outra organização para a mesma tabela não são afetadas. Se a invalidação falhar, a requisição falha em vez de deixar entradas obsoletas.

## Atualidade e carga: quem decide {: #freshness-and-load-who-decides }

Três partes influenciam o quão recente é um resultado e quanta carga uma consulta coloca nos sistemas por trás dela.

- **A fonte de origem.** Idealmente, gerencia a própria contrapressão: limites de conexão, tempos limite de instrução, réplicas de leitura.
- **O operador.** Protege a plataforma, o próprio Provisa, da contrapressão. Quando uma fonte não consegue se proteger, o operador a protege também. As configurações ficam em fontes e tabelas: `load_protected`, `replicate`, os `federation_hints` da fonte, o limite de redirecionamento de resultados grandes, o cluster Kafka em que os sinks escrevem e o watermark registrado de cada tabela.
- **O usuário final.** Equilibra velocidade e atualidade, uma requisição por vez.

As configurações do operador são o piso. Uma requisição pode ir acima dele, rumo a dados mais antigos ou menos carga; ativar o cache de respostas é o exemplo comum. Nunca pode ir abaixo. Uma dica de requisição que faria isso é recusada com um erro que nomeia a configuração do operador, sem ser aplicada nem descartada em silêncio.

| Entrada da requisição | O que pode fazer | Recusada quando |
| --- | --- | --- |
| `@route(engine: DIRECT)`, `-- @provisa route=direct` | Escolher o driver direto para uma fonte | uma tabela lida pela instrução é `load_protected` ou `replicate: 0` |
| `@join`, `@reorder`, `@broadcastSize`, `/*+ ... */` | Definir uma propriedade de sessão do motor que os `federation_hints` da fonte deixaram livre | altera uma propriedade definida pelo operador |
| `X-Provisa-Redirect-Threshold`, `@redirect(threshold:)` | Redirecionar um resultado mais cedo | é maior que o limite do operador |
| `@sink(broker:)`, broker de `X-Provisa-Sink` | Repetir o broker do operador | nomeia outro broker, ou não há `KAFKA_BOOTSTRAP_SERVERS` configurado |
| `@watermark` em uma assinatura | Repetir o watermark registrado da tabela | nomeia outra coluna, ou a tabela não tem nenhum |
| `@cached`, `-- @provisa cache=true` | Servir um resultado mais antigo do cache | nunca; uma entrada de cache nunca é mais recente que a leitura que a gravou |

As recusas retornam HTTP 403 com o código `query.operator_floor`, SQLSTATE `42501` via pgwire e `PERMISSION_DENIED` via Flight e gRPC.

Uma tabela `load_protected` ou com `replicate: 0` nunca é lida ao vivo por uma consulta, em nenhum transporte. As consultas leem sua réplica. Só a replicação e suas atualizações leem a fonte: uma atualização ocorre quando `cache_ttl` ou uma verificação de atualidade a exige; sem nenhum dos dois, a réplica é construída uma vez e depois só é atualizada por um feed de mudanças ou pelo agendador. Numa tabela assim com uma `off_peak_window` (na tabela ou na sua fonte, lida em `off_peak_tz`), uma atualização só roda enquanto a janela está aberta: uma que vence ou é pedida fora dela espera a próxima abertura da janela, e uma janela sem `cache_ttl` atualiza uma vez por abertura. A primeira construção nunca é retida, porque as leituras da tabela esperam por ela.

### A configuração `replicate` {: #the-replicate-setting }

`replicate` é um inteiro em uma tabela. Uma tabela sem valor assume o de sua fonte e, quando nenhum dos dois está definido, vale o limite global. Uma réplica é uma cópia de uma tabela de origem mantida no armazenamento do motor e atualizada conforme as configurações de tempestividade da tabela; não é um fluxo contínuo de mudanças.

| Valor | Nome | Significado |
| --- | --- | --- |
| não definido | Padrão | Vale o limite global |
| `-1` | Never | Lida ao vivo onde existir um caminho ao vivo. Replicada apenas em um motor que não consegue ler a fonte no local |
| `N` maior que 0 | Hot-N | Lida ao vivo até a tabela passar de N instruções governadas por intervalo; depois, servida pela sua réplica |
| `0` | Always | As leituras vêm da réplica |

Só Always é uma garantia. Never e os valores Hot são de melhor esforço. `load_protected` com `replicate: -1` é recusado no carregamento da configuração e ao salvar. Uma fonte com `replicate: 0` não tem attach ao vivo em nenhum motor, de modo que toda tabela dela é servida pela sua réplica.

O piso é avaliado pelas tabelas que uma instrução lê. Uma instrução que não lê nenhuma tabela servida por réplica mantém sua rota direta mesmo quando a fonte tem outras tabelas replicadas.

Uma réplica é indexada pela chave primária declarada e pelas colunas em que os relacionamentos declarados fazem junção, com índices construídos depois que ela pousa. Onde esses índices aceleram junções, onde manter réplicas e como escolher a atualidade delas estão em [Replicas](replicas.md).

### Replicação hot {: #hot-replication }

Uma tabela cujo `replicate` não está definido, ou é `N` maior que 0, é lida ao vivo até ficar movimentada.

```yaml
replication:
  hot_threshold: 100       # statements per interval for a table left at Default
  hot_interval: 60         # seconds: the window the count is taken over, and how often it is judged
  hot_max_rows: 10000000   # a larger table is not replicated for being busy
```

| Configuração | Variável de ambiente | Padrão |
| --- | --- | --- |
| `replication.hot_threshold` | `PROVISA_REPLICATION_HOT_THRESHOLD` | 100 |
| `replication.hot_interval` | `PROVISA_REPLICATION_HOT_INTERVAL` | 60 |
| `replication.hot_max_rows` | `PROVISA_REPLICATION_HOT_MAX_ROWS` | 10000000 |

As três são configurações do operador: a interface de administração as mostra em Hot Tables, e um valor salvo vale a partir da próxima avaliação.

Como uma tabela é contada:

- Uma instrução governada conta cada tabela que lê uma única vez, não importa quantas vezes a leia.
- Uma instrução atendida pelo cache de respostas não conta. Ela não colocou carga na fonte.
- Uma instrução recusada pela governança não conta.

Uma tabela é promovida quando sua contagem atinge o limite dela (o `N` próprio, senão o da fonte, senão `hot_threshold`) e rebaixada quando a contagem cai abaixo da metade dele. A promoção solicita a construção de uma réplica; a tabela é lida ao vivo até a construção terminar, depois pela réplica. O rebaixamento devolve as leituras à fonte imediatamente.

Uma tabela não é promovida quando tem mais linhas que `hot_max_rows`, quando seu sinal de mudança é `ttl` ou `ttl_probe` e nem ela nem sua fonte declaram `cache_ttl`, ou quando a camada hot do Redis já a gerencia. O resumo da tabela na interface de administração informa qual caso se aplica.

As contagens ficam no Redis. Com um Redis compartilhado (`cache.redis_url`), cobrem todos os processos da implantação. Com o Redis embutido, são por processo; assim, a promoção só ocorre quando um único worker atende as requisições; com vários workers e sem Redis compartilhado nada é promovido, e o resumo informa isso.

## Autenticação {: #authentication }

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

### Tipos de provedor de autenticação {: #auth-provider-types }

| Provedor | Caso de uso | Validação de token |
| ---------- | ---------- | ----------------- |
| `simple` | Desenvolvimento/testes locais. Usuários definidos em YAML. | JWT assinado com `PROVISA_JWT_SECRET` |
| `firebase` | Firebase Authentication (todos os métodos). | `verify_id_token()` do SDK `firebase-admin` |
| `keycloak` | Keycloak OIDC. Tenant + funções de cliente mapeadas. | Validação de JWT baseada em JWKS |
| `oauth` | OIDC genérico (Okta, Azure AD, Auth0, PingFederate). | JWKS a partir da URL de descoberta |
| `basic` | Implantações autônomas. As contas ficam no armazenamento próprio do Provisa. | Senha bcrypt, ou SCRAM-SHA-256 no pgwire |

Credenciais de superusuário (bloco `superuser`) funcionam com qualquer provedor e sempre resolvem para a função admin com todas as capacidades. Usadas para configuração inicial antes que a autenticação externa seja configurada.

### SCRAM-SHA-256 (`auth.scram`)

```yaml
auth:
  provider: basic
  scram: true
```

Faz o pgwire anunciar SASL com `SCRAM-SHA-256`, para que a senha seja provada e nunca enviada em texto puro. Vale só para o provedor `basic`, porque nenhum outro provedor guarda os verificadores RFC 5802 de que o SCRAM precisa, e o channel binding não é oferecido.

Verificadores não podem ser derivados dos hashes bcrypt existentes. Um é gravado sempre que uma senha passa em texto plano, então a primeira conexão SCRAM de cada usuário vem após seu próximo cadastro, login, troca de senha ou redefinição pelo administrador. Até lá, as conexões desse usuário recaem na troca em texto claro sobre TLS; o fio não revela quem já migrou.

### Limitação de tentativas de login (`auth.login_throttle`) {: #login-throttling-authlogin_throttle }

```yaml
auth:
  login_throttle:
    max_attempts: 5      # failures within the window before lockout
    window_seconds: 300  # how far back failures are counted
    lockout_seconds: 900 # how long a locked-out subject is refused
```

Ativa por padrão com os valores exibidos; o bloco apenas os ajusta. O contador fica na camada de validação de credenciais, então falhas por HTTP, pgwire e Bolt somam-se contra o mesmo sujeito e um bloqueio vale em todas as interfaces. É por processo: vários workers da API permitem, cada um, até `max_attempts`.

### Tokens de acesso pessoal {: #personal-access-tokens }

PATs não precisam de bloco de configuração. Eles são sempre aceitos, e o armazenamento deles é criado com o restante do esquema do plano de controle. O que se configura é a expiração que um usuário pode pedir ao emitir um: de 1 a 366 dias, ou nenhuma para um token que não expira. Veja [Modelo de segurança](security.md#personal-access-tokens).

### TLS mútuo {: #mutual-tls }

A verificação de certificados de cliente é configurada por variáveis de ambiente, junto das configurações de certificado TLS que ela estende, e não tem lugar em `provisa.yaml`.

| Variável | Padrão | Significado |
| ---------- | --------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | não definida | Pacote PEM da(s) AC autorizada(s) a assinar certificados de cliente. Defini-la ativa a verificação de certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` assim que uma AC é definida | `required` ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exige que o common name do certificado seja igual ao nome de usuário com que a conexão se autentica |

Cada uma aceita uma sobrescrita por protocolo, com os mesmos nomes das configurações de TLS. Um modo definido sem CA, ou um modo que não é nem `required` nem `optional`, impede a inicialização, para que o servidor nunca atenda conexões que o operador acredita verificadas sem que estejam.

### Endereçar uma organização sobre TLS {: #addressing-an-org-over-tls }

Nada a configurar. Em uma implantação multiorganização, pgwire e Bolt leem a organização a partir do nome de host discado pelo cliente, transportado no ClientHello do TLS, exatamente como o HTTP a lê do cabeçalho `Host`. Um cliente que se conecta a `acme.provisa.dev` solicita a organização `acme`; a solicitação é recusada a menos que o principal autenticado seja membro. Conectar-se por endereço IP não solicita organização alguma, o que é o caso de toda conexão em uma implantação de organização única.

### Exemplo de configuração completa de autenticação (comentado) {: #full-auth-config-example-commented-out }

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

## Mutações Upsert {: #upsert-mutations }

Para tabelas com chave primária, o Provisa gera campos de mutation `upsert_<table>`. Eles compilam para um upsert no dialeto de destino: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...` no PostgreSQL, `ON DUPLICATE KEY UPDATE` no MySQL.

```graphql
mutation {
  upsert_orders(objects: [{id: 1, amount: 150.00, region: "us"}]) {
    affected_rows
  }
}
```

Colunas de conflito são derivadas dos metadados de PK. Todas as regras de visibilidade de coluna e permissão de escrita se aplicam.

## Distinct On

O argumento `distinct_on` seleciona a primeira linha para cada valor distinto das colunas especificadas. Disponível nos campos de consulta raiz.

```graphql
{
  orders(distinct_on: [region], order_by: [{region: asc, created_at: desc}]) {
    region
    amount
    created_at
  }
}
```

Compila para `SELECT DISTINCT ON (region) ...` no PostgreSQL. Para dialetos não-PG, um fallback com função de janela é usado.

## Presets das permissões de escrita {: #write-grant-presets }

Uma permissão de escrita define colunas por conta própria em insert e update. Os presets são declarados na lista `presets` da permissão, por função e operação.

```yaml
tables:
  - source_id: sales-pg
    domain_id: sales-analytics
    schema: public
    table: orders
    columns:
      - {name: amount, visible_to: [analyst]}
      - {name: created_by, visible_to: [analyst]}
      - {name: created_at, visible_to: [analyst]}
      - {name: source_system, visible_to: [analyst]}
    write_grants:
      - role: analyst
        operation: insert
        columns: [amount]
        presets:
          - {column: created_by, source: session, name: user_id}   # from the request's session
          - {column: created_at, source: now}                      # time of the write
          - {column: source_system, source: literal, value: provisa}
```

| Origem | Comportamento |
| -------- | ---------- |
| `session` | Injeta o valor da variável de sessão nomeada (`name`) da requisição |
| `now` | Injeta o momento da escrita |
| `literal` | Injeta uma constante (`value`) |

Os presets são adicionados à instrução durante a compilação, antes da geração do SQL. A instrução não pode fornecer uma coluna com preset, e uma coluna com preset não está no tipo de entrada da mutação.

## Funções Herdadas {: #inherited-roles }

Funções podem herdar de uma função pai via `parent_role_id`. A cadeia é achatada na inicialização. Uma função filha detém a união das capacidades e do acesso a domínio de seus ancestrais; uma coluna, métrica, função ou webhook concedido a um ancestral é concedido à filha; e as regras de RLS de um ancestral se aplicam à filha por tabela, função mais próxima primeiro, com a própria regra da filha para uma tabela substituindo a do pai.

```yaml
roles:
  - id: admin
    description: Administers everything.
    capabilities: [admin]
    domain_access: ["*"]
  - id: analyst
    description: Queries the sales analytics domain.
    capabilities: [query_development]
    domain_access: [sales-analytics]
  - id: junior_analyst
    description: A new analyst, with the analyst's access and nothing more.
    capabilities: []
    domain_access: []
    parent_role_id: analyst      # inherits query_development + sales-analytics
  - id: intern
    description: An intern, with the junior analyst's access.
    capabilities: []
    domain_access: []
    parent_role_id: junior_analyst  # inherits from junior_analyst (and transitively analyst)
```

Herança em múltiplos níveis é suportada. As capacidades e domain_access explícitos da função filha são mesclados com os da função pai. Um pai precisa ser uma função existente, não pode ser a própria função e não pode fechar um ciclo; cada caso é recusado ao salvar.

## Gatilhos Programados {: #scheduled-triggers }

Gatilhos baseados em cron que chamam uma URL de webhook em uma programação. Usa o APScheduler.

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

Tarefas programadas são gerenciadas via a UI de administração (alternância habilitar/desabilitar) ou a mutação de administração `toggle_scheduled_task`.

Um gatilho pode disparar sobre uma tabela registrada em vez de uma programação: `on: {table, event}` nomeia a tabela como `source_id.schema.table` e o evento, `change`, `error` ou `quarantine`. Um gatilho `command` executa um comando registrado como sua `role`, pelo despacho governado do próprio comando, e lhe passa como argumentos o que o disparou. Uma mudança passa `table` e, onde o sinal de mudança o carrega (Kafka, Debezium), o lote carregado como `changes`. Um evento `error` ou `quarantine` passa `table`, `event`, `reason` e `error`. Um gatilho cujo comando declara um argumento que o gatilho não passa é recusado ao salvar. Uma mudança dispara uma vez por rajada que seu sinal de mudança informa, ou quando chega uma escrita feita pelo Provisa; uma tabela servida de sua réplica dispara quando uma construção carrega linhas diferentes da anterior. A própria escrita de um comando é uma mudança, então os gatilhos se encadeiam. Cada execução é registrada com seu horário, seu resultado (`succeeded`, `refused` ou `failed`), seu erro e o que a disparou, e a consulta de administração `scheduledTasks` a lista. Só os gatilhos de prod disparam, e nada é instalado em um banco de dados de origem.

```yaml
scheduled_triggers:
  - id: bronze_to_silver
    on: {table: bronze_pg.public.bronze, event: change}
    command: promote_to_silver
    role: loader
  - id: alert_on_quarantine
    on: {table: bronze_pg.public.bronze, event: quarantine}
    command: notify_steward      # declares arguments table, reason
    role: loader
```

## Formato OrderBy {: #orderby-format }

OrderBy usa o formato `{column: direction}` com um enum de direção de 6 valores:

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
| ----------- | ----- |
| `asc` | `ASC` |
| `desc` | `DESC` |
| `asc_nulls_first` | `ASC NULLS FIRST` |
| `asc_nulls_last` | `ASC NULLS LAST` |
| `desc_nulls_first` | `DESC NULLS FIRST` |
| `desc_nulls_last` | `DESC NULLS LAST` |

Ordenação por relacionamento é suportada via objetos aninhados:

```graphql
{
  orders(order_by: [{customers: {name: asc}}]) {
    id
    customers { name }
  }
}
```

## Observabilidade {: #observability }

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

### Filtros de Telemetria {: #telemetry-filters }

O Provisa tem dois caminhos de exportação OTLP independentes: o seu collector interno e o endpoint de suporte opcional do Provisa. Cada caminho tem seu próprio filtro. Os filtros rodam antes de os spans saírem do processo, e os objetos span originais nunca são alterados.

**`telemetry_filter`** — controla o que chega ao seu coletor interno.

| Chave | Tipo | Padrão | Descrição |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `false` | Substitui literais string e numéricos em `db.statement` por `?` |
| `redact_attributes` | list[str] | `[]` | Chaves de atributo removidas completamente de todo span |

**`support_telemetry_filter`** — controla o que chega ao endpoint de suporte Provisa. A redação de literais SQL tem padrão `true` neste caminho, já que os dados de consulta pertencem a você.

| Chave | Tipo | Padrão | Descrição |
| ----- | ------ | --------- | ------------- |
| `redact_sql_literals` | bool | `true` | Substitui literais string e numéricos em `db.statement` por `?` |
| `redact_attributes` | list[str] | `[]` | Chaves de atributo removidas completamente de todo span |

Um `db.statement` com redação. Com `redact_sql_literals: true`, este atributo de span:

```yaml
db.statement: SELECT * FROM orders WHERE region = 'us-west' AND amount > 500
```

torna-se:

```yaml
db.statement: SELECT * FROM orders WHERE region = ? AND amount > ?
```

### Endpoint de Suporte {: #support-endpoint }

`support_endpoint` (ou a variável de ambiente `PROVISA_SUPPORT_OTLP_ENDPOINT`) encaminha telemetria ao suporte do Provisa para diagnóstico. Quando não está definido, nenhum dado sai da sua infraestrutura por esse caminho. O filtro de suporte funciona independente do interno, então você pode ocultar literais SQL nas duas exportações e ainda compartilhar com o suporte os tempos dos spans e os dados de erro.

### Detecção de Protocolo do Endpoint {: #endpoint-protocol-detection }

O Provisa seleciona OTLP/HTTP ou OTLP/gRPC a partir do esquema da URL do endpoint. URLs que começam com `http://` ou `https://` usam OTLP/HTTP, com `/v1/traces`, `/v1/metrics` e `/v1/logs` anexados automaticamente. Qualquer outro esquema usa OTLP/gRPC com `insecure=True`.

## Motor de Federação {: #federation-engine }

Configurar um motor de federação é opcional. O padrão é `duckdb`: sem configuração, no próprio processo, sem serviço externo. Escolha outro motor quando precisar de escala MPP ou quiser reaproveitar um warehouse que já tem.

Precedência: variável de ambiente `PROVISA_ENGINE` → campo de configuração `federation_engine` persistido na UI de administração → `duckdb`. As mudanças têm efeito na reinicialização do serviço.

### Visão geral dos motores {: #engine-overview }

| Chave do motor | Rótulo | Dialeto | MPP | Mecanismo de link externo | Autenticação |
| ----------- | ------- | --------- | ----- | ------------------------ | ------ |
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
| `mysql` | MySQL | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `mariadb` | MariaDB | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `oracle` | Oracle Database | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `mssql` | Microsoft SQL Server | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `db2` | IBM Db2 | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `redshift` | Amazon Redshift | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `greenplum` | Greenplum | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `cockroachdb` | CockroachDB | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `yugabytedb` | YugabyteDB | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `opengauss` | openGauss | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `tidb` | TiDB | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `singlestore` | SingleStore | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `vertica` | Vertica | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `exasol` | Exasol | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `teradata` | Teradata Vantage | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `saphana` | SAP HANA | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `sapase` | SAP ASE (Sybase) | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `sqlanywhere` | SAP SQL Anywhere | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `monetdb` | MonetDB | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `firebird` | Firebird | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |
| `sqlalchemy` | Outro banco de dados relacional (por URL de conexão) | Por dialeto | Não | Nenhum (apenas pouso) | Credenciais por dialeto |

### Referência de motores {: #engine-reference }

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

Em processo; sem serviço externo. O motor padrão. `PROVISA_DATA_DIR` controla onde o armazenamento embutido vive (`~/.provisa` por padrão).

```bash
PROVISA_ENGINE=duckdb   # or omit — this is the default
```

O armazenamento de materialização padrão é `~/.provisa/materialize.duckdb`. É o único motor cujo armazenamento padrão não é PostgreSQL.

#### clickhouse (embutido) / clickhouse-server {: #clickhouse-embedded-clickhouse-server }

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

#### Motores de banco de dados relacional (mysql, mariadb, oracle, mssql, db2, redshift, greenplum, cockroachdb, yugabytedb, opengauss, tidb, singlestore, vertica, exasol, teradata, saphana, sapase, sqlanywhere, monetdb, firebird) e `sqlalchemy` {: #relational-database-engines-mysql-mariadb-oracle-mssql-db2-redshift-greenplum-cockroachdb-yugabytedb-opengauss-tidb-singlestore-vertica-exasol-teradata-saphana-sapase-sqlanywhere-monetdb-firebird-and-sqlalchemy }

Uma chave por banco de dados relacional endereçável pela rede, todos no mesmo runtime apenas-pouso (sem federação para fontes externas): cada fonte pousa no armazenamento e é consultada ali. A chave seleciona o banco de dados; `PROVISA_ENGINE_URL` carrega o DSN que seu dialeto espera. `sqlalchemy` é o catch-all para um banco de dados sem chave própria. Armazenamentos embutidos em arquivo (SQLite, Access) não são oferecidos, porque o servidor precisa ser alcançável pela rede.

```bash
PROVISA_ENGINE=mysql
PROVISA_ENGINE_URL="mysql+pymysql://user:pass@host:3306/db"
```

O armazenamento de materialização usa `TENANT_DATABASE_URL` como padrão.

### Armazenamento de materialização {: #materialization-store }

Quando uma fonte não pode conectar (attach) ao vivo (sem conector ATTACH para o motor selecionado), ela pousa no armazenamento de materialização do motor. Ordem de resolução: `PROVISA_MATERIALIZE_URL` explícito → padrão declarado do motor → erro explícito (sem fallback silencioso).

O DuckDB declara seu arquivo embutido (`~/.provisa/materialize.duckdb`) como padrão. Todos os outros motores usam `TENANT_DATABASE_URL` (PostgreSQL) como padrão. Sobreponha qualquer motor com `PROVISA_MATERIALIZE_URL`.

O armazenamento decide o que os índices de uma réplica fazem pelas junções: um armazenamento PostgreSQL indexa as réplicas e um motor que executa SQL nele lê esses índices, enquanto o DuckDB faz junções das réplicas do seu arquivo embutido sobre uma cópia por instrução. Veja [Replicas](replicas.md#use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it).

### Hints de federação por fonte {: #per-source-federation-hints }

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

Para fontes do Google Cloud, defina `GOOGLE_APPLICATION_CREDENTIALS` com o caminho do arquivo de chave da sua conta de serviço. Para Fabric e Synapse, entre com `az login` (desenvolvimento) ou uma identidade gerenciada (produção); o motor lê as credenciais pelo `DefaultAzureCredential` do `azure-identity`.

## Variáveis de Ambiente {: #environment-variables }

| Variável | Padrão | Descrição |
| ---------- | --------- | ------------- |
| `PROVISA_CONFIG` | `config/provisa.yaml` | Caminho do arquivo de configuração |
| `TENANT_DATABASE_URL` | `postgresql+psycopg://provisa:provisa@localhost:5432/provisa` | URI do armazenamento do plano de controle (SQLAlchemy async); aceita `sqlite+pysqlite://…` / `duckdb://…` para o armazenamento desktop embutido |
| `PLATFORM_DATABASE_URL` | — | URI do registro de plataforma (diretório de tenants, registro de motores); obrigatório na inicialização, sem fallback |
| `PROVISA_REDIS_EMBEDDED` | — | `1`/`true` usa fakeredis embutido em vez de um servidor Redis — sem Docker |
| `PG_HOST` | `localhost` | Host PostgreSQL |
| `PG_PORT` | `5432` | Porta PostgreSQL |
| `PG_DATABASE` | `provisa` | Banco de dados PostgreSQL |
| `PG_USER` | `provisa` | Usuário PostgreSQL |
| `PG_PASSWORD` | `provisa` | Senha PostgreSQL |
| `PROVISA_ENGINE` | `duckdb` | Chave do motor de federação |
| `PROVISA_ENGINE_URL` | — | URL de conexão para motores orientados a URL (Snowflake, Databricks, ClickHouse Server, BigQuery, SQLAlchemy) |
| `PROVISA_MATERIALIZE_URL` | — | Sobrepõe o DSN do armazenamento de materialização (padrão é o declarado pelo motor) |
| `PROVISA_DATA_DIR` | `~/.provisa` | Diretório de dados para o armazenamento DuckDB embutido |
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
| `PROVISA_RETRY_BUDGET_SECS` | `30` | Orçamento de retry de leitura de Tier-1 em segundos; backoff exponencial com jitter completo |
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
| `PROVISA_MTLS_CLIENT_CA` | — | Pacote PEM da(s) AC autorizada(s) a assinar certificados de cliente; defini-la ativa a verificação de certificados de cliente em pgwire, Bolt, gRPC e Flight |
| `PROVISA_MTLS_MODE` | `required` assim que uma AC é definida | `required` ou `optional`; qualquer outro valor recusa-se a iniciar |
| `PROVISA_MTLS_BIND_PRINCIPAL` | `false` | Exige que o common name do certificado seja igual ao nome de usuário que se autentica |
| `PROVISA_BOLT_ALLOWED_ORIGINS` | — | Sites separados por vírgula autorizados a abrir um WebSocket Bolt a partir de um navegador; não definida recusa toda origem de navegador |
| `PROVISA_EXTRAS` | `firebase,vector` | Extras do pyproject embutidos na imagem da aplicação; `scripts/provisa` os deriva de `dq_checker` em `~/.provisa/config.yaml` |
| `PROVISA_DQ_CHECKER` | `none` | Somente instalador: `none`/`soda`/`gx`, lido pelo `first-launch.sh` em modo não interativo e gravado em `config.yaml` como `dq_checker` |
| `ANTHROPIC_API_KEY` | — | Chave de API Claude (descoberta) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Sobrepõe `observability.endpoint` |
| `OTEL_SERVICE_NAME` | `provisa` | Sobrepõe `observability.service_name` |
| `OTEL_LOG_LEVEL` | `WARNING` | Sobrepõe `observability.log_level` |
| `OTEL_COMPACT_BATCH_SIZE` | `10` | Sobrepõe `observability.compact_batch_size` |
| `OTEL_SPAN_EXPORT_DELAY_MILLIS` | `1000` | Atraso de flush do processador de spans em lote |
| `PROVISA_SUPPORT_OTLP_ENDPOINT` | — | Sobrepõe `observability.support_endpoint` |
