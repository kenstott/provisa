# Réplicas

Uma réplica é uma cópia de uma tabela de origem que o Provisa mantém no armazenamento de materialização do motor e lê no lugar da fonte. Se uma tabela é replicada ou não é definido por [`replicate`](configuration.md#the-replicate-setting) e pelo que o motor consegue alcançar; veja [What an engine changes](engines.md#what-an-engine-changes).

## As réplicas são indexadas {: #replicas-are-indexed }

O Provisa indexa uma réplica da forma como o modelo usa a tabela dela.

- **A chave primária declarada**, imposta como chave.
- **As colunas em que os relacionamentos declarados fazem junção**:
  - a chave estrangeira de cada relacionamento cuja origem é esta tabela;
  - a coluna de destino de cada relacionamento cujo destino é esta tabela, a menos que essa coluna já seja a chave;
  - as duas chaves estrangeiras de uma tabela de junção (junction) percorrida por um relacionamento.

Considere este relacionamento:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

A réplica de `orders` recebe a sua chave e um índice em `customer_id`. A réplica de `customers` recebe a sua chave e, quando `id` é essa chave, não precisa de mais nada. Uma junção das duas lê então um índice nos dois lados, em um armazenamento que usa índices para junções (veja [What each store builds](#what-each-store-builds)). Um relacionamento sobre uma coluna calculada que a réplica não mantém não recebe índice.

Declare a chave com `is_primary_key: true` na coluna. Uma réplica sem chave declarada recebe apenas os índices de junção.

### Legível primeiro, indexada depois {: #readable-first-indexed-after }

Um pouso nunca espera pelos índices. A réplica é lida desde o momento em que é trocada; o executor de réplicas (replica runner) constrói então os índices dela, um de cada vez, em uma passagem separada, sob um bloqueio de índice por réplica. Espere que as primeiras consultas após um pouso rodem sem eles.

Cada pouso registra os índices que pediu, os construídos e os que falharam, pelo nome, no registro da réplica no plano de controle (`replica_state.index_state`).

- **Um índice que falha nunca faz o pouso falhar.** Ele é registrado no log com a causa e pedido de novo pelo pouso seguinte. A linha de log é `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`.
- **Linhas que repetem uma chave declarada são uma falha à parte**, com o código `replica.index_duplicate_key`, nomeando as colunas da chave. A fonte contém duplicatas de um conjunto de colunas que o modelo declara único. Tentar de novo não resolve: corrija os dados na fonte ou corrija a chave declarada.
- **Um pouso que substitui a tabela no meio de uma construção** deixa para trás as construções anteriores; elas param e não registram nada, e a nova tabela recebe as suas. Em um armazenamento PostgreSQL, a troca cancela uma construção ainda em andamento na tabela que ela substitui.
- **O plano acompanha o modelo a cada pouso.** Um relacionamento adicionado depois recebe o seu índice no pouso seguinte da réplica.

A interface de administração não mostra o registro de índices. Leia o log do servidor ou a coluna `index_state` da linha da réplica.

### O que cada armazenamento constrói {: #what-each-store-builds }

| Armazenamento | O que é construído | Como |
| --- | --- | --- |
| PostgreSQL: o banco do próprio motor `pg`, o armazenamento PostgreSQL do Trino ou do DuckDB, o motor `sqlalchemy` sobre PostgreSQL | Índices de chave e de junção | `CREATE INDEX CONCURRENTLY` em conexão própria, de modo que leitores e escritores não são bloqueados. A chave é construída como índice único e depois transformada em chave primária com `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. Um índice inválido deixado por uma construção cancelada é descartado e construído de novo. |
| Outros bancos por trás de um motor relacional (`mysql`, `oracle`, `mssql` e os demais) | Índices de chave e de junção | O próprio DDL do dialeto. Um banco que recusa o DDL registra o índice como falho. |
| Arquivo DuckDB embutido, MotherDuck | Índices de chave e de junção | Pelo broker do armazenamento: `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | Apenas a chave de ordenação | Uma réplica do ClickHouse é ordenada pela chave declarada (`ORDER BY`), definida quando ela pousa. O ClickHouse não tem índice B-tree secundário a construir. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Nada | O data warehouse organiza a tabela por conta própria. |

Um índice só ajuda uma junção que roda onde o índice está. Dois motores leem o seu armazenamento sem usar os índices dele para junções:

- **DuckDB com o arquivo embutido.** O motor nunca anexa o arquivo. Cada instrução lê a sua própria cópia local de toda tabela do armazenamento que cita, refeita sempre que o armazenamento mudou. As junções rodam como hash joins sobre essa cópia, então os índices de junção não as aceleram; os índices do arquivo impõem a chave e atendem o que lê o arquivo no local.
- **DuckDB com um armazenamento PostgreSQL.** O DuckDB lê o armazenamento por meio da extensão `postgres` com o filter pushdown desligado (bug upstream duckdb/duckdb-postgres#564), de modo que varre a réplica e filtra e faz junções no DuckDB. Os índices do armazenamento são construídos, mas essas leituras não os usam.

Um armazenamento MotherDuck é anexado e lido no local.

## Use um banco relacional como armazenamento das réplicas onde o motor executa SQL nele {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Quando o motor executa as suas junções dentro de um banco relacional, mantenha as réplicas ali. As junções leem então os índices acima. Três configurações fazem isso:

- **O motor `pg`.** As réplicas pousam no banco PostgreSQL do próprio motor e cada instrução roda ali, pelos índices delas.
- **Um motor relacional** (`mysql`, `oracle`, `mssql`, o genérico `sqlalchemy` e os demais). Toda fonte pousa nesse banco e é consultada ali.
- **Trino com o seu armazenamento PostgreSQL.** PostgreSQL é o único armazenamento que o Trino suporta; o Trino lê as réplicas pelo conector PostgreSQL. Um filtro ou junção que o Trino empurra para o PostgreSQL usa os índices; o trabalho que o próprio Trino faz não usa.

Aponte um motor para um armazenamento com `PROVISA_MATERIALIZE_URL` ou `materialize_store_url`; o armazenamento registrado por uma organização tem precedência sobre ambos.

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

Esta é uma recomendação geral, não uma regra. Se compensa depende do motor, das fontes, do armazenamento e do tamanho das tabelas. Pese estes pontos:

- **Alcance por consulta.** Um armazenamento que o motor alcança pela rede acrescenta uma ida e volta e uma transferência a cada leitura. O arquivo DuckDB embutido é local e colunar, e varre rápido.
- **Custo do pouso.** Gravar uma réplica em um banco relacional e depois construir os índices dela custa mais que gravar um arquivo local.
- **A carga do próprio banco.** Leituras de réplica, pousos e construções de índice rodam todos nesse banco. Dimensione-o para eles.
- **Varreduras grandes.** Um índice não faz nada por uma consulta que lê a maior parte de uma tabela grande. Um armazenamento colunar a varre mais rápido que um armazenamento de linhas.

## Escolhendo onde as réplicas ficam {: #choosing-where-replicas-live }

O armazenamento é uma alavanca entre várias, e elas se sobrepõem. Resolva primeiro as outras; o armazenamento decide apenas o que elas deixam.

| Alavanca | O que faz | Onde ajuda |
| --- | --- | --- |
| Tabelas hot | Uma tabela pequena de consulta é mantida no Redis e colocada em cada consulta que a lê como uma CTE (common table expression) `VALUES` embutida. | Junções com pequenas tabelas de consulta, em qualquer armazenamento: as linhas viajam dentro da consulta. O destino de um relacionamento muitos-para-um, ou uma tabela com no máximo `hot_tables.auto_threshold` linhas (1.000 por padrão), torna-se hot na primeira leitura. `hot: true` força a entrada de uma tabela e `hot: false` a mantém fora. |
| Exibições materializadas que você define | Uma exibição com `materialize: true` é armazenada e atualizada. Uma consulta que lê a exibição é atendida pelas linhas armazenadas quando ela está atual e quem lê a vê inteira: nenhuma entrada carrega uma regra de linha mais estreita que tudo. | Agregações sobre tabelas grandes. Você decide quais existem; nenhuma é criada para você. Veja [Views](configuration.md#views-governed-computed-datasets). |
| Relacionamento com `materialize: true` | Um relacionamento entre fontes ganha uma exibição pai-filho armazenada e pré-unida. Uma requisição cuja junção corresponde ao padrão, total ou parcialmente, é reescrita para lê-la, quer chegue como SQL, quer seja construída pelo compilador. | Junções em um armazenamento que não constrói índices de junção. Veja [Auto-Materialization](configuration.md#auto-materialization). |
| Posicionamento (Placement) | Fontes que o motor consegue ler ao vivo são lidas ao vivo; as demais são replicadas. O que acaba no armazenamento decorre do motor escolhido. | A escolha do motor decide a maior parte do que o armazenamento precisa guardar. |

A materialização de relacionamentos e as junções indexadas respondem à mesma necessidade de duas maneiras. Em um armazenamento colunar ou de data warehouse (o arquivo DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) nenhum índice de junção é construído, então uma exibição pré-unida é o modo de baratear uma junção frequente. Em um armazenamento relacional, as colunas de junção já estão indexadas, e as mesmas junções podem rodar rápido sem materializá-las.

O que ainda depende do armazenamento: varreduras e junções ad hoc sobre tabelas grandes que nenhuma exibição materializada cobre e nenhuma tabela hot atende. Elas rodam sobre os índices do armazenamento em um banco relacional, ou sobre a velocidade de varredura dele em um colunar.

Os resultados variam conforme a carga de trabalho. Meça antes e depois de uma mudança com a [ferramenta de dimensionamento](sizing.md); as [execuções ao vivo-ou-réplica](sizing.md#live-or-replica-reads-and-the-ttl) dela dão o custo de uma leitura de réplica frente ao de uma leitura ao vivo na sua implantação.

## Atualidade {: #freshness }

Faça da atualidade parte da decisão, ao lado da velocidade. Quão antiga uma resposta pode ser, e o que custa à fonte mantê-la atual, decidem se uma tabela é lida ao vivo ou de uma réplica e, portanto, o que o armazenamento precisa guardar.

Fontes corporativas raramente suportam tempo real. Os dados chegam por carga em lote ou ETL noturno, os feeds de mudanças atrasam e muitos sistemas não aguentam leituras ao vivo no volume das consultas. Parta da atualidade de que cada tabela precisa e depois escolha a estratégia mais barata que a atenda. As regras do lado do operador estão em [Freshness and load: who decides](configuration.md#freshness-and-load-who-decides).

| Estratégia | Definida com | O que garante | O que custa à fonte |
| --- | --- | --- | --- |
| Leitura ao vivo | `replicate: -1` | Atual no momento da consulta | Toda consulta lê a fonte |
| TTL | `change_signal: ttl`, `cache_ttl` | Uma réplica não mais antiga que `cache_ttl` | Uma reconstrução por `cache_ttl`; com um `watermark_column`, apenas as linhas além do watermark |
| Sonda de mudança | `change_signal: probe` ou `ttl_probe`, `cache_ttl` | Reconstruída só depois que a sonda informa uma mudança; a sonda roda a cada `cache_ttl` | Uma sonda por intervalo e uma reconstrução só quando há mudança |
| Feed de mudanças por push | Uma fonte Kafka ou WebSocket; `change_signal: debezium` numa fonte com bloco `cdc`; `change_signal: native` no PostgreSQL, MongoDB ou Cosmos DB | Atual dentro do atraso do feed e do debounce | Kafka, WebSocket e Debezium: nada após a primeira construção, o feed carrega as linhas. PostgreSQL, MongoDB e Cosmos DB: uma reconstrução por rajada de mudanças |
| Sinal externo | `change_signal: signal` | Reconstruída sempre que `POST /data/sources/{source_id}/signal` informa que a fonte mudou; nunca por relógio | Uma reconstrução por sinal |
| TTL por papel | `role_ttl` em uma tabela | Um papel listado aceita uma réplica até o seu próprio TTL; o `cache_ttl` da tabela continua sendo o piso | Menos reconstruções: a leitura de um papel tolerante não pede nenhuma |
| Proteção de carga | `load_protected: true`, com `cache_ttl`, uma sonda ou uma `off_peak_window` | As consultas nunca leem a fonte; só o executor atualiza a réplica, e com uma janela fora de pico só enquanto ela está aberta | Apenas as atualizações agendadas |
| Freshness gate | `freshness_gate: true` em uma fonte | Cada leitura é antes conferida com o `change_signal` e o `cache_ttl` da fonte; um veredito de desatualizada atualiza antes da leitura | Uma atualização no caminho de leitura quando desatualizada |
| Nunca expira | `cache_ttl: -1` em uma fonte ou tabela | Carregada uma vez; toda leitura é respondida pela réplica, e a leitura de uma tabela ainda não carregada é recusada pelo nome | Apenas essa carga: uma fonte cujas tabelas foram todas carregadas nunca mais é anexada, sondada ou lida, e não precisa de credencial |

Uma sonda que não tem com o que comparar (sem watermark, sentinel ou consulta de sonda) degrada para o TTL.

### Quase tempo real a partir de um feed de mudanças {: #near-real-time-from-a-change-feed }

Quando uma fonte não aguenta carga de consultas ao vivo, mas já publica suas mudanças, um feed de mudanças dá dados quase em tempo real sem tocar a fonte no momento da consulta. As consultas leem a réplica local, que é rápida e não coloca carga na fonte, enquanto o laço de eventos aplica as mudanças conforme chegam. Os dados atrasam apenas o que o feed atrasa, tipicamente segundos.

Ter o feed não basta. O Provisa só o segue para uma tabela ligada a ele:

- **Uma fonte Kafka.** A tabela nomeia o seu tópico, e cada mudança é aplicada à réplica pela chave primária, inclusive exclusões.

    ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Uma fonte WebSocket.** O seu fluxo é aplicado da mesma forma.
- **Uma tabela com `change_signal: debezium`** (na tabela ou na sua fonte) numa fonte cujo bloco `cdc` indica o transporte Kafka e o prefixo de topic do conector. A primeira leitura constrói a réplica; depois disso, cada evento de mudança no topic `{topic_prefix}.{schema}.{table}` é aplicado por chave primária, exclusões incluídas, e o offset só é confirmado depois de a mudança ter sido carregada.
- **Uma tabela de uma fonte assim com `change_signal: kafka`**. As suas mudanças são lidas do topic de deltas que o seu `live.kafka.topic` indica, pelo mesmo transporte, e aplicadas da mesma forma; uma tabela neste sinal que não indica nenhum topic é recusada pelo nome ao salvar a tabela ou a sua fonte e ao carregar a configuração.
- **Uma tabela MongoDB ou Cosmos DB com `change_signal: native`** (na tabela ou na fonte dela). Cada rajada de mudanças no change stream pede uma reconstrução da réplica.
- **Uma tabela PostgreSQL com `change_signal: native`** (na tabela ou na sua fonte). O Provisa lê a replicação lógica do banco a partir de um slot próprio, `provisa_<source id>`, sobre uma publicação de mesmo nome que lista as tabelas nativas da fonte. O Kafka não participa. Cada rajada de mudanças numa tabela pede uma reconstrução da sua réplica. O banco precisa do mesmo que o Debezium: `wal_level` definido como `logical`, um papel com o direito de replicação que possa criar a publicação, e um primário em vez de uma réplica de leitura; uma fonte à qual falte algum deles é recusada pelo nome ao salvar e ao carregar a configuração. O slot e a publicação são criados quando a primeira tabela os segue e removidos quando a última deixa de segui-los ou a fonte é excluída. Ao iniciar, um servidor verifica cada fonte PostgreSQL registrada e remove um slot do Provisa que nenhuma tabela nativa siga, de modo que um slot deixado por uma configuração alterada enquanto nenhum servidor rodava não continue retendo o WAL da fonte.

Uma tabela por push precisa de chave primária declarada; uma sem ela é ignorada e registrada no log.

Uma tabela com sinal de mudança por push conta como atual no caminho de leitura, porque o feed dela é a sua verificação de atualidade. Por isso, um sinal de push que nada receberia é recusado: `change_signal: native` em qualquer tipo que não seja Kafka, WebSocket, ingest, PostgreSQL, MongoDB ou Cosmos DB é recusado ao salvar a fonte ou a tabela e ao carregar a configuração, assim como `debezium` numa fonte sem bloco `cdc`.
