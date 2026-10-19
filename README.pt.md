# Provisa

**Conecte seus bancos de dados. Consulte-os em 5 minutos com GraphQL, gRPC, SQL ou MCP, por qualquer API ou protocolo.**

O Provisa coloca uma única API sobre todos os seus dados. Aponte-o para seus bancos de dados e aplicações de negócio, descreva como eles se encaixam, e consulte o resultado combinado por REST, GraphQL, SQL, gRPC, MCP, JSON:API, Arrow Flight, pgwire ou Bolt.

Funciona porque tudo vem de um único modelo: seus domínios, os relacionamentos entre suas tabelas e as políticas sobre elas, em todas as fontes. O Provisa roda a partir desse modelo e governa a partir dele, então não há nada para manter em sincronia. Os relacionamentos registrados são os únicos joins que uma consulta pode fazer, e as políticas de acesso são compiladas em todo plano de consulta. Quatro coisas decorrem do modelo único:

- **Federação.** 73 conectores alcançam mais de 85 tipos de fonte, entre eles Salesforce, ServiceNow e Stripe. Os sistemas SaaS em que seu negócio roda são lidos como tabelas ao lado dos seus bancos, no lugar, sem um pipeline para um data warehouse antes. Consulte-os em SQL, GraphQL ou Cypher, por nove protocolos.
- **Métricas.** Uma métrica é um agregado nomeado e governado. Você a define uma vez, e toda ferramenta e linguagem que a pede recebe a mesma definição.
- **Ontologia.** O glossário de negócio é um grafo de conceitos, das suas definições e de como se relacionam. O Provisa o mapeia para as tabelas e colunas que guardam os valores, e é isso que permite responder perguntas sobre seus dados. Um conceito continua rascunho até chegar aos dados, diretamente ou por outros conceitos. O glossário diz o que um conceito é; uma métrica diz como um número é calculado.
- **Governança.** Seis camadas de verificação são compiladas em todo plano de consulta, em toda linguagem e por todo protocolo, e toda requisição é registrada do mesmo jeito: quem consultou o quê, como qual papel, sob qual política. Traces, métricas e logs são registrados como tabelas consultáveis ao lado dos seus dados de negócio.

Você pode perguntar em **GraphQL, Cypher ou SQL** e se conectar por **pgwire, Bolt, gRPC, REST, Arrow Flight ou JDBC**. Toda linguagem de consulta é reduzida a uma única representação intermediária, e a governança é aplicada ali, uma vez, para que uma política não possa divergir entre linguagens. Na saída, essa forma é traduzida para o dialeto de cada fonte. Uma linguagem de consulta nova é um front-end novo sobre o mesmo núcleo.

Leituras e escritas passam por ele. Uma consulta entre fontes se espalha pela camada de federação. Escritas, e leituras de uma única fonte, vão direto ao driver dessa fonte, governadas do mesmo jeito e em geral de volta em menos de 100 ms. Streaming colunar do Arrow Flight vem embutido.

O modelo tem um vocabulário pequeno: domínios, relacionamentos, papéis e políticas. Isso o mantém fácil de ler e de auditar, e você pode ler o conjunto de políticas e saber o que ele faz. O Provisa em si é um compilador de consultas leve. Ele nunca fica no seu caminho de dados como runtime. Ele transforma uma requisição em consultas nativas, as roteia e sai do caminho, e é por isso que o patrimônio de dados continua rápido.

Você pode usá-lo de duas formas, e elas se combinam:

- **Para modernizar.** Modele seu acervo, deixe o Provisa gerar o SQL nativo de cada fonte e depois adote esse SQL no sistema de destino. O Provisa é a ponte da transição, e você pode dispensá-lo depois.
- **Para governar de vez.** Mantenha-o como o caminho de toda consulta, para que definição, aplicação e auditoria fiquem juntas enquanto o acervo existir.

## Experimente uma demo {: #try-a-demo }

O jeito mais rápido é uma demo ao vivo no navegador, sem instalar nada: [provisa.dev/demos](https://provisa.dev/demos). Cada uma abre um sandbox privado com um tour guiado.

A partir de uma cópia deste repositório, você pode executar qualquer demo na sua própria máquina:

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

Uma demo publicada é baixada como um pacote pronto; as outras são executadas a partir de `demo/named/<name>/`. Veja [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) para todas as opções.

## O modelo de federação {: #the-federation-model }

Quatro ideias sustentam todo o desenho. Toda fonte é reduzida a tabelas. Toda consulta é reduzida a uma forma parecida com SQL. A alcançabilidade decide o que é consultado ao vivo e o que é copiado antes. E uma estratégia de atualização mantém cada cópia, e cada conjunto de dados derivado, tão atual quanto precisa ser.

Comece pelas fontes. Cada uma é expressa como um conjunto de tabelas bidimensionais sobre um único sistema de tipos compartilhado, e esse é o único contrato que uma fonte precisa cumprir para entrar no acervo. Uma tabela MySQL ou PostgreSQL já é uma. O resultado de uma consulta GraphQL vira uma quando é achatado. Triplestores SPARQL e o Neo4j não parecem nada com tabelas, mas você fornece uma consulta com resultado tabular, e a consulta vira o adaptador. Seja qual for a fonte, o acervo vê linhas, colunas e tipos. Trazer um novo tipo de fonte é cumprir esse único contrato, às vezes com uma pequena ajuda humana. Ninguém escreve uma integração sob medida.

As consultas recebem o mesmo tratamento. O SQL, com todos os seus dialetos, é a linguagem da análise sobre tabelas, e isso faz de uma forma parecida com SQL o destino comum natural. Então toda requisição, em qualquer linguagem que chegue, é reduzida primeiro a essa forma. SQL e GraphQL se reduzem com facilidade. Os caminhos e a semântica de grafos do Cypher dão trabalho de verdade, mas chegam lá. Como tudo passa por uma forma antes de qualquer outra coisa, a governança se aplica em um só lugar, seja qual for a linguagem da requisição.

Com fontes tabulares e uma só forma de consulta, a federação pode cobrir tanto a consulta ao vivo quanto o data warehousing: o que um motor de consulta ao vivo como o Trino faz, mais a materialização em que esses motores se apoiam. A **alcançabilidade** junta as duas coisas. O motor consegue consultar uma fonte onde ela está, ou os dados dela precisam ser copiados antes para um lugar consultável? A resposta divide o acervo entre o que é lido ao vivo e o que é copiado.

A maioria dos bancos já tem algum tipo de vínculo ao vivo: o `ATTACH` do DuckDB, o `postgres_fdw` do PostgreSQL, os links externos do Databricks. Então a maioria dos bancos pode atuar como motor de federação até certo ponto. Nenhum cobre tudo. Cada um alcança um conjunto específico de fontes e copia o resto, e nenhum diz claramente o que é o quê. O modelo torna a alcançabilidade explícita, como um conjunto definido de métodos por fonte que dizem o que o motor alcança ao vivo e, por exclusão, o que precisa ser copiado.

Resta a atualização. Para cada fonte que o motor não alcança, o quanto a cópia dela precisa estar atual? Na prática há poucas respostas: sob demanda, em um agendamento, em um sinal de mudança (CDC, uma marca d'água, um snapshot) ou fixada. Escolher uma por fonte é toda a política de atualização.

Conjuntos de dados derivados cabem na mesma forma: tabelas derivadas, agregados, a saída de uma transformação. Eles são escritos na mesma forma, então a linhagem vem de graça. O caminho de cada sistema de origem até uma saída final é a definição que a produziu, legível de ponta a ponta. Construí-los leva a pergunta da atualização um passo adiante: o conjunto é atualizado em um agendamento, só quando suas pré-condições são atendidas, continuamente, ou como um snapshot histórico fixado? As opções são o mesmo pequeno conjunto da cópia de uma fonte, então a política de construção de um conjunto derivado usa o mesmo vocabulário.

Modelos dimensionais decorrem diretamente disso. Fatos e dimensões de um esquema estrela são conjuntos derivados como quaisquer outros. Uma dimensão é uma projeção conformada e sem duplicatas, e uma tabela de fatos é um join com agregação reduzido a um grão, cada um com sua política de construção e de atualização. Dimensões de mudança lenta não precisam de nada especial: um snapshot fixado é histórico de Tipo 2, uma reconstrução agendada é Tipo 1. Como o esquema é definido no modelo e não fica preso às tabelas de um warehouse, os mesmos fatos e dimensões podem ser construídos no Oracle, no Databricks, ou ficar virtuais sobre um motor MPP, sem remodelar.

O Data Vault se encaixa do mesmo jeito, uma camada antes. Hubs são conjuntos sem duplicatas de chaves de negócio, links são os relacionamentos registrados entre eles, e satélites são conjuntos de atributos só de inserção, com carimbo de tempo, que guardam o histórico. Um satélite é simplesmente um conjunto derivado na estratégia de sinal de mudança: data de carga mais hashdiff é captura de mudanças aplicada a atributos descritivos, e histórico só de inserção é a estratégia do snapshot fixado. Tabelas point-in-time e bridge são mais conjuntos derivados, construídos para a velocidade da consulta. Então um raw vault é um conjunto de dados derivados no modelo, e um esquema estrela é uma projeção dele, ambos gerados e ambos portáveis entre motores. A metodologia continua sendo decisão sua: o que vira hub, o grão de um satélite, como dividir. Depois de decidido, a decisão vive no modelo. Nenhum ETL a solda a um warehouse.

Você declara os dois padrões com **dois atalhos** em vez de views escritas à mão. Eles são os blocos de que todo esquema estrela e todo Data Vault são feitos, e não prendem você a nenhum dos dois métodos:

- **`entity`** é uma projeção de uma fonte com chave, sem duplicatas, que pode guardar histórico. Declare a chave, os atributos e um modo de histórico, e o Provisa constrói uma view materializada. Peça histórico e ele constrói em vez disso uma **MV bitemporal** (`scd2` → delta, `snapshot` → snapshot). Uma única construção dá a você uma **dimensão** de Kimball (SCD1 ou SCD2) ou um **hub com satélite** de Data Vault.
- **`fact`** se une a chaves de entidade, se reduz a um grão declarado e agrega medidas. O Provisa constrói uma MV de agregado mais os relacionamentos registrados com as entidades. Uma única construção dá a você uma **tabela de fatos** em estrela ou um **link** de Data Vault; um fato sem medidas é um link de puro conjunto de chaves.

Cada `entity` ou `fact` vira exatamente as views materializadas, as configurações bitemporais e os relacionamentos que um modelador escreveria à mão, então o warehouse fica no modelo e muda de motor sem remodelar. Declare um na interface de administração (o formulário **Model**) ou pela API de administração (`registerEntity` / `registerFact`). O modelo gera para você uma estrela de Kimball ou um Data Vault; não impõe nenhum dos dois.

### Viagem no tempo {: #time-travel }

Viajar no tempo significa guardar cada versão de uma linha em vez de sobrescrevê-la, para que você possa perguntar como os dados eram em qualquer momento passado. Os motores diferem muito na eficiência com que fazem isso, então o Provisa torna isso parte da **definição** da view materializada, seja qual for o motor de armazenamento. Declare uma vez e funciona em qualquer armazenamento capaz de guardar uma view materializada.

O que o mantém portável é ser **só de acréscimo**. Uma versão, uma vez gravada, nunca é atualizada nem apagada. O truque bitemporal usual de fechar uma linha regravando uma data de "válido até" exige um UPDATE, que muitos motores não fazem de forma barata, ou nem fazem, sobre um armazenamento federado. Então cada atualização **acrescenta**, e qual versão valia em um dado momento é descoberto na leitura, a partir do log. Há dois jeitos de acrescentar:

- **Snapshot** acrescenta o conjunto inteiro recém-obtido, carimbado com a hora de sistema da atualização. Não há comparação de diferenças e é correto em todo motor, mas o armazenamento cresce uma cópia inteira por atualização.
- **Delta** acrescenta só o que mudou, mais marcas de exclusão para as chaves removidas. O próprio motor calcula o delta, com anti-joins dentro de um `INSERT … SELECT`. O Provisa nunca o junta linha a linha. É menor e precisa de uma chave de entidade.

O Provisa gerencia assim o tempo de sistema, quando ele registrou uma versão. O tempo de validade, quando um fato é verdade no negócio, vem do próprio SELECT da view e é mantido. Motores que fazem melhor, como snapshots nativos do Iceberg ou um MERGE que toca menos linhas, podem ser usados por trás da mesma declaração; o caminho só de acréscimo é o piso correto em toda parte.

A leitura não precisa de sintaxe especial. Uma consulta simples em uma MV bitemporal devolve o estado **atual**, reconstruído a partir do log. Para voltar no tempo, envie um cabeçalho `X-Provisa-As-Of: <timestamp>` e a consulta inteira é respondida como o acervo estava naquele momento, do mesmo jeito em todo armazenamento. Acrescente `X-Provisa-As-Recorded` para ler os dados como o Provisa os tinha registrado em um momento, para que uma correção registrada depois não mude a reprodução do passado. Ative isso para qualquer view materializada na interface de administração (o controle **Time Travel**: desligado, snapshot ou delta, mais uma chave de entidade) ou pela API de administração.

Alcançabilidade e atualização juntas formam um modelo geral de federação: uma definição do que é ao vivo, do que é copiado e de quão atual cada cópia fica, independente do que qualquer motor alcança. O modelo é portável, então o seu acervo não fica preso ao fornecedor cuja federação hoje alcança mais fontes.

## Recursos {: #features }

### Interfaces de consulta {: #query-interfaces }

Estas são as linguagens e APIs estruturadas em que você escreve consultas. Cada uma tem sua própria sintaxe e semântica; a governança (RLS, mascaramento, visibilidade de coluna, aplicação de relacionamento) se aplica uniformemente a todas elas, independentemente de qual protocolo de rede as entrega.

- **GraphQL** — Esquemas por papel com visibilidade por campo, filtragem, paginação por cursor e consultas de agregação (`count`, `sum`, `avg`, `min`, `max`). O esquema oferece só relacionamentos registrados, então toda consulta é estruturalmente válida por construção, e o GraphQL é o caminho mais rápido para uma consulta simples correta. O Apollo APQ vem incluído: as consultas viram hash e são registradas no servidor, as chamadas seguintes enviam só o hash por HTTP GET, e as respostas passam a ser cacheáveis em CDN sem mudança no cliente. Tabelas de consulta abaixo de um limite de linhas configurável aparecem como tipos enum.
- **SQL** — SQL completo sobre dados federados, sem restrições e mais expressivo que o GraphQL. Escreva SQL padrão, subconsultas correlacionadas incluídas, e ele roda entre fontes sem mudança. Uma consulta de fonte única pula totalmente a camada de federação (menos de 100 ms).
- **Cypher** — Linguagem de consulta a grafos sobre o mesmo esquema federado. Percorra relacionamentos como arestas de grafo; una fontes; caminhos de comprimento variável. A governança se aplica de forma idêntica a GraphQL e SQL.
- **API de modelo gRPC** — Um `.proto` gerado a partir do esquema registrado, com RPCs tipadas de consulta e inserção para cada tabela na raiz do esquema de um papel, e respostas em streaming. Como o GraphQL, ela é guiada pelo esquema: o modelo de registro é o contrato, e o protobuf é a codificação de rede. É uma interface de consulta completa por tabela, enquanto o Arrow Flight é um transporte de streaming colunar.
- **JSON:API** — Uma API de consulta estruturada em `/data/jsonapi/{table}`, só HTTP por design. Suporta JSON:API 1.1: conjuntos de campos esparsos (`fields[table]=col1,col2`), expressões de filtro (`filter[field][op]=value`), documentos compostos (`include=relation`) e ordenação. Ela consulta uma tabela por vez com uma sintaxe de filtro padrão, então use SQL ou GraphQL quando precisar de uma linguagem de consulta de uso geral.
- **Explorador de linguagem de consulta** — Escreva uma consulta GraphQL e veja traduções ao vivo de **SQL Semântico** e **Cypher** em painéis laterais; copie qualquer uma delas ou vá direto ao editor de SQL ou de Grafo. Um fluxo de trabalho prático é esboçar fragmentos de consulta em GraphQL, depois costurar o SQL resultante em views ou relatórios complexos.

O Explorer mostra uma consulta GraphQL ao lado de suas traduções ao vivo em SQL e Cypher:

![Query Language Explorer](docs/images/query-explorer.png)

O mesmo esquema federado também aparece como um grafo vivo, com rótulos de domínio e de nó, tipos de relacionamento e travessias de comprimento variável:

![Graph Visualization](docs/images/graph-view.png)

### Ferramentas de composição de consulta {: #query-composition-tools }

Estas ferramentas ajudam você a escrever consultas nas linguagens acima. Elas mesmas não são linguagens de consulta.

- **Consulta em linguagem natural** — Um pipeline NL→SQL/Cypher/GraphQL que roda no Claude. Descreva o que você quer em linguagem simples, e o pipeline escreve uma consulta na linguagem escolhida, com um ciclo de validação interativo antes de qualquer execução.

![Natural Language Query](docs/images/natural-language.png)

### Protocolos de rede {: #wire-protocols }

Estes são os protocolos de conexão. SQL, GraphQL e Cypher trafegam por eles, e o protocolo escolhido não muda nem a interface de consulta nem o comportamento da governança.

- **pgwire** — Qualquer cliente PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` do pandas) conecta na porta 5439 como se fosse um servidor Postgres. Aceita apenas SQL. O pipeline completo de governança se aplica. `pg_catalog` e `information_schema` são respondidos a partir de um catálogo em memória, então navegadores de esquema funcionam sem uma ida e volta de federação. TLS opcional.
- **Bolt (Neo4j)** — Qualquer cliente Neo4j (Neo4j Browser, Bloom, drivers oficiais) conecta via protocolo Bolt e executa Cypher contra o grafo federado. Cada função que o usuário possui aparece como um banco de dados `provisa_<role>`. Mesma governança que qualquer outro transporte. TLS opcional.
- **Arrow Flight** — Streaming colunar de alta vazão sobre gRPC; aceita GraphQL ou SQL como entrada de consulta. Conjuntos de resultados ilimitados, sem materialização no servidor, sem infraestrutura separada necessária.
- **JDBC** — Integração com ferramentas de BI (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Subscriptions: eventos de mudança quase em tempo real; backends: PG nativo, MongoDB nativo, CDC, polling. Também exposto via Kafka.

### Fontes de dados {: #data-sources }

- **73 conectores, mais de 85 tipos de fonte** — O número maior conta um a um os serviços PostgreSQL hospedados (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) e os formatos de arquivo do File Crawler. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets, Kaggle, Salesforce, ServiceNow, Stripe e o restante ficam atrás de uma única API. Fontes de grafo e RDF ficam no mesmo patamar das relacionais.
- **Roteamento inteligente** — Uma consulta de fonte única pula a federação (menos de 100 ms). Uma consulta de várias fontes passa pela camada de federação, no seu próprio cluster ou nos workers embarcados.
- **Fontes de API** — Registre endpoints REST, GraphQL, gRPC, WebSocket ou RSS como tabelas consultáveis, com auxiliares SPARQL incluídos. Joins federados entre fontes de API e fontes relacionais não exigem configuração extra.
- **Introspecção de esquema remoto** — Aponte para qualquer endpoint GraphQL, OpenAPI ou gRPC; operações documentadas são automaticamente expostas como tabelas consultáveis, nós de grafo e arestas com governança completa aplicada por cima
- **Fontes de arquivo** — Arquivos CSV, Parquet e SQLite como tabelas consultáveis; suporta caminhos locais e armazenamento de objetos remoto (`s3://`, `ftp://`, `sftp://`)
- **Conjuntos de dados Kaggle** — busque o catálogo público do Kaggle e registre um conjunto de dados por busca ao vivo autenticada por token, sem download manual; registra como uma única fonte de arquivo cujas tabelas são descobertas automaticamente, somente pacotes CSV/Parquet
- **Integração Kafka** — Tópicos como tabelas somente-leitura; resultados de consulta como sinks Kafka
- **Gatilhos programados** — Gatilhos cron e por intervalo (APScheduler) que disparam webhooks, mutações ou publicações em sinks Kafka
- **Dicas de desempenho de federação** — Dicas de roteamento via comentário SQL sobrepõem decisões automáticas de roteamento

![Data Sources](docs/images/data-sources.png)

Fontes, arquivos e endpoints remotos são registrados como tabelas governadas a partir da UI:

![Table Registration](docs/images/table-registration.png)

### Segurança e Governança {: #security-governance }

- **Segurança em nível de linha** — Injeção de cláusula WHERE por tabela, por função
- **Mascaramento de coluna** — Mascaramento por coluna (regex, constante, truncamento) com contorno baseado em função
- **Presets de coluna** — Valores estáticos ou de variável de sessão do lado do servidor, injetados em insert/update; não expostos nos tipos de entrada de mutação
- **Permissões de escrita** — Controle de acesso de mutação por coluna (`writable_by`)
- **Funções herdadas** — Funções herdam RLS, visibilidade e mascaramento de uma função pai recursivamente
- **Funções e webhooks rastreados** — Funções de BD e webhooks de saída expostos como mutações GraphQL com formas de retorno tipadas
- **Hook de aprovação ABAC** — Hook de autorização pré-execução; transporte webhook, gRPC ou unix_socket; escopo por tabela, por fonte ou global; política de fallback configurável
- **Autenticação plugável** — Firebase, Keycloak, OAuth 2.0, simples (testes)

![Security Roles](docs/images/security-roles.png)

### Entrega e desempenho {: #delivery-performance }

- **Views materializadas como transformações registradas** — Uma MV registra a transformação que a produziu: sua forma de join ou seu SQL, os sinais de entrada por fonte a partir dos quais foi construída (snapshot do Iceberg, watermark de RDB) e uma verificação de determinismo no registro. Como a transformação está registrada, o Provisa pode reescrever uma consulta, ou parte dela, sobre uma MV atualizada sem que você mude a consulta. A correspondência trabalha sobre padrões de join e aceita correspondências parciais, então uma MV que cobre parte dos joins ainda se aplica, e os joins restantes são mantidos.
- **Inlining de tabelas quentes** — Tabelas de consulta pequenas e frequentemente unidas são embutidas como CTEs VALUES diretamente no plano de consulta, eliminando idas e voltas entre fontes para dados de dimensão
- **Cache de consulta** — Cache de resultado Redis particionado por função+RLS; cache de hash APQ incluído
- **Observabilidade como dado** — Traces distribuídos, métricas e logs são coletados via OpenTelemetry, compactados em Iceberg no S3 e registrados como tabelas consultáveis (`traces`, `metrics`, `logs`, `queries`) no esquema federado. Consulte-os com SQL, GraphQL ou Cypher ao lado dos seus dados de negócio. Junte, por exemplo, uma tabela `customers` a `queries` para ver quem rodou o quê e quanto tempo levou.

### Administração e integração {: #administration-integration }

- **API de administração** — GraphQL em `/admin/graphql`; upload/download de configuração, edição de relacionamento, aprovação de consulta
- **Visualizador de relatórios** — `/admin/reports` lista as visões de gestão integradas do domínio de operações e quaisquer relatórios personalizados registrados; exige a capacidade `observability`
- **Prévia de tabela** — toda tabela registrada tem um visualizador de dados governado com paginação no servidor, filtros empurrados para a fonte, agrupamento em múltiplos níveis e exportação CSV
- **GraphQL Voyager** — Visualização interativa de esquema, com escopo por função, como diagrama entidade-relacionamento
- **Descoberta de relacionamento por LLM** — O Claude sugere candidatos a chave estrangeira.
- **Cliente Python** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → Tables pyarrow, dialeto SQLAlchemy, suporte a ADBC
- **Ingestão de dados** — Endpoints HTTP para enviar dados de evento JSON para a plataforma
- **Importação Hasura v2 / DDN** — Converta metadados Hasura v2 ou YAML de supergraph DDN em configuração Provisa
- **Apollo Federation** — Exponha o Provisa como um subgraph Apollo Federation v2

Esquema com escopo por função visualizado como diagrama entidade-relacionamento (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Relacionamentos são registrados, aprovados e aplicados como os únicos caminhos JOIN legais:

![Relationships](docs/images/relationships.png)

## Modelo de segurança {: #security-model }

É aqui que "no caminho que toda consulta já percorre" precisa se sustentar. O Provisa aplica um único modelo de segurança em camadas a toda linguagem de consulta (GraphQL, SQL, Cypher) e a todo transporte (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket), e nenhum caminho de consulta o contorna. A cobertura vem do desenho, então ninguém precisa se lembrar de nada: adicione uma fonte, uma coluna ou um relacionamento, e cada camada se aplica a ele na hora, sem nada a mais para registrar.

As camadas se aplicam em ordem, e uma requisição precisa passar por cada uma antes de a próxima ser verificada.

### Camada 0 — Filtragem de introspecção {: #layer-0-introspection-filtering }

O esquema e o catálogo que um papel vê contêm só as tabelas da sua lista `domain_access` e as colunas que passam nas regras `visible_to`. Tudo fora do acesso do papel é invisível desde o início: não dá para consultar, autocompletar, nem deduzir que existe. Isso vale para o esquema GraphQL, o catálogo SQL e o navegador de esquema do editor de consultas.

### Camada 1 — Acesso público {: #layer-1-public-access }

Tabelas em domínios sem restrição `domain_access` são visíveis para toda identidade autenticada, sem nada mais a configurar. Dados que são de fato públicos não dão trabalho.

### Camada 2 — Acesso a domínio {: #layer-2-domain-access }

Cada papel carrega uma lista `domain_access` de IDs de domínio, e uma consulta que toca uma tabela fora desses domínios é recusada antes de rodar. É a fronteira ampla de propriedade: um papel de RH não alcança tabelas de finanças, seja qual for a escrita do SQL.

### Camada 3 — Segurança em nível de linha {: #layer-3-row-level-security }

Confirmado o acesso ao domínio, predicados `WHERE` por tabela e por papel são acrescentados a cada `SELECT` na execução e avaliados sobre os dados brutos. Uma gerente regional que consulta uma tabela de pedidos compartilhada vê só as linhas da sua região, mesmo com `SELECT *`.

### Camada 4 — Visibilidade e mascaramento de coluna {: #layer-4-column-visibility-and-masking }

Colunas cuja lista `visible_to` deixa de fora o papel solicitante são removidas da saída. Colunas com regra de mascaramento têm os valores trocados por ocultação via regex, uma constante ou truncamento antes de os resultados saírem do servidor. O mascaramento vale em toda linguagem de consulta e todo formato de saída.

### Camada 5 — Guarda de predicado {: #layer-5-predicate-guard }

Colunas mascaradas são recusadas em cláusulas `WHERE` e `HAVING`. Do contrário, quem consulta poderia descobrir o valor real por busca binária num filtro, mesmo com a saída mascarada. A recusa acontece na análise da consulta, antes de qualquer execução.

### Governança de relacionamento {: #relationship-governance }

Em SQL, uma condição JOIN precisa corresponder a um relacionamento registrado e aprovado entre as tabelas, e um join não aprovado é recusado. Cada relacionamento traz um motivo e uma descrição legíveis, que dizem a pessoas e agentes autônomos por que aquele caminho existe. É uma política de governança e não uma fronteira de segurança rígida. As camadas 2 a 5 se mantêm seja qual for a forma do join, então um desvio proposital não expõe nada que o papel não alcançaria com duas consultas separadas, e cada tentativa fica registrada para auditoria.

Nenhum join inválido roda nos seus dados. Os papéis são governados por padrão, e sair disso significa conceder de propósito o direito `ignore_relationships` a um papel de confiança. O token OAuth de um agente corresponde a um papel, então um agente recebe as mesmas camadas que uma pessoa. Catálogos de dados com uma camada de contexto dizem a um agente o que ele deveria fazer; o Provisa impõe o que ele pode fazer.

---

Essas camadas se combinam. Um papel com acesso a domínios, RLS e colunas mascaradas tem todas as camadas ativas ao mesmo tempo, porque cada camada é configurada à parte e vale para qualquer consulta que toque um objeto governado.

## Início rápido {: #quick-start }

### macOS

1. Baixe [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (sempre a versão mais recente)
2. Arraste **Provisa.app** para `/Applications` e clique duas vezes para abrir
3. A primeira execução completa uma configuração única (~2 min, sem internet necessária)
4. Abra o Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Baixe [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (sempre a versão mais recente)
2. Torne-o executável e execute-o. A primeira execução completa uma configuração única (sem internet necessária):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Baixe [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (sempre a versão mais recente)
2. Execute o instalador. Ele não exige privilégios de administrador.
3. Abra **Provisa First Launch** no Menu Iniciar. Ele completa uma configuração única (~5 min, sem internet necessária).
4. Abra um novo terminal:

```bash
provisa start
```

### Primeira consulta {: #first-query }

No desenvolvimento local (`PROVISA_MODE=test`), nenhuma credencial é necessária. Em produção, você se autentica com um token Bearer, e o Provisa lê o papel a partir dele.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Baixe [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre a versão mais recente) e adicione-o ao caminho de drivers da sua ferramenta de BI.

```text
jdbc:provisa://localhost:8815
```

Autentique-se com seu usuário e senha do Provisa, e o servidor atribui o seu papel.

Veja [docs/integrations.md](docs/integrations.md) para os passos de configuração do Tableau e Power BI.

### Protocolo de rede PostgreSQL (pgwire) {: #postgresql-wire-protocol-pgwire }

O Provisa fala o protocolo de rede do PostgreSQL na porta 5439. Qualquer cliente que consiga se conectar ao Postgres se conecta ao Provisa, sem driver, sem adaptador e sem mudar suas ferramentas atuais.

**O nome de usuário PostgreSQL seleciona a função Provisa.** Com `provider: none` (modo trust), a senha é ignorada e qualquer nome de função configurado é aceito como nome de usuário — conecte como `analyst`, `admin`, ou qualquer função para ver a visão governada dos dados dessa função. Com `provider: simple`, a senha é validada por bcrypt. Outros provedores (`firebase`, `keycloak`, `oauth`) não são suportados via pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Toda consulta passa pelo pipeline de governança completo, então acesso a domínios, RLS, mascaramento e proteção de predicados valem exatamente como em GraphQL e REST. Navegadores de esquema (DBeaver, DataGrip, pgAdmin) funcionam sem configuração. O Provisa responde às consultas `pg_catalog` e `information_schema` deles a partir de um catálogo em memória limitado ao acesso a domínios do papel, então os usuários veem só as tabelas e colunas que podem consultar.

O DataGrip navegando pelo esquema governado e seu diagrama de chaves estrangeiras via pgwire, sem driver e sem adaptador:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

O TLS é habilitado configurando `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY`. A porta é configurável via `PROVISA_PGWIRE_PORT` (padrão `5439`).

### Bolt (protocolo de rede Neo4j) {: #bolt-neo4j-wire-protocol }

O Provisa também fala o protocolo **Bolt** do Neo4j, então ferramentas nativas de grafo se conectam diretamente e rodam Cypher contra o grafo federado. Nada é exportado, e você não precisa de um banco de grafos separado. Aponte o **Neo4j Browser** ou o **Bloom** para o Provisa e percorra relacionamentos entre fontes sob a mesma governança (acesso a domínios, RLS, mascaramento).

Aqui o Neo4j Browser executa Cypher contra o Provisa. Rótulos de nó, tipos de relacionamento e chaves de propriedade vêm direto do esquema registrado:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Ative-o definindo `PROVISA_BOLT_PORT` (o padrão do Neo4j é `7687`), e ligue o TLS com `PROVISA_BOLT_CERT` e `PROVISA_BOLT_KEY`. Cada papel do Provisa que o usuário autenticado tem aparece como um banco de dados selecionável `provisa_<role>` (o seletor `provisa_admin` acima). Escolher um restringe a sessão aos direitos de domínio desse papel, e o usuário nunca pode ir além dos papéis que tem.

### Cliente Python {: #python-client }

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Veja [docs/python-client.md](docs/python-client.md) para a referência completa.

## Documentação {: #documentation }

| Tópico | Doc |
| --- | --- |
| Início rápido para desenvolvedores (rodando a partir do código-fonte) | [docs/quickstart.md](docs/quickstart.md) |
| Referência completa de configuração YAML | [docs/configuration.md](docs/configuration.md) |
| Referência de endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Design de sistema e mapa de componentes | [docs/architecture.md](docs/architecture.md) |
| Modelo de segurança (RLS, mascaramento, autenticação) | [docs/security.md](docs/security.md) |
| Armazenamento de segredos e referências `${secret:NAME}` | [docs/secrets.md](docs/secrets.md) |
| Glossário de negócios e curadoria de termos | [docs/glossary.md](docs/glossary.md) |
| Ambientes (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Criar ambientes de teste (perfilamento, dados falsos, dados sintéticos) | [docs/test-data.md](docs/test-data.md) |
| Referência dos métodos de dados falsos | [docs/fake-methods.md](docs/fake-methods.md) |
| Executar e criar demos em um notebook (engenheiros de vendas) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Tipos de fonte suportados | [docs/sources.md](docs/sources.md) |
| Subscriptions SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, ferramentas de BI, clientes Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Cliente Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API de administração | [docs/admin.md](docs/admin.md) |
| Implantação (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Importação Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Fluxo de trabalho de release (tags alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamento {: #sizing }

O Provisa inclui um motor de federação embutido para consultas de múltiplas fontes. Na primeira execução você escolhe um orçamento de RAM; o Provisa deriva o número de workers de federação locais automaticamente.

| RAM do host | Workers | Carga de trabalho típica |
| --- | --- | --- |
| < 24 GB | 0 | Desenvolvimento, consultas de fonte única, equipes pequenas |
| 24–47 GB | 1 | Equipe pequena, consultas moderadas entre fontes |
| 48–95 GB | 2 | Implantação departamental, uso misto de BI + notebook |
| 96 GB+ | 4 | Departamento grande, federação concorrente pesada |

O número de workers pode ser alterado a qualquer momento editando `~/.provisa/config.yaml` (`federation_workers: N`) e executando `provisa restart`. Defina como `0` para rodar apenas coordenação (nó único).

### Escalando além de uma única máquina {: #scaling-beyond-a-single-box }

**Escalonamento horizontal** — Rode várias instâncias do Provisa atrás de um balanceador de carga. Cada instância é um sistema completo. Todas precisam apontar para o mesmo banco de configuração (defina `CONFIG_DB_HOST` nas máquinas secundárias), e podem compartilhar uma instância Redis (`REDIS_URL`) para um cache único. A maioria das consultas se distribui sem mudança. Um join entre fontes muito grande pode passar do que uma instância aguenta, e então você precisa de uma máquina maior ou de um cluster de federação externo.

**Redis compartilhado** — Defina `REDIS_URL` em cada instância para apontar para um Redis externo. Redis compartilhado significa que entradas de cache de uma instância ficam disponíveis para todas, melhorando as taxas de acerto em todo o cluster.

**Traga seu próprio cluster de federação** — Aponte o Provisa para um cluster de federação externo existente em vez dos workers embutidos. Recomendado para implantações de grande escala ou em nuvem; veja [docs/deployment.md](docs/deployment.md) para configuração.

## Licença {: #license }

Business Source License 1.1 (sem modificações, conforme os pactos de Licenciante da MariaDB). Cada
versão lançada converte-se para a Change License (GPL v2.0 ou posterior) no 4º
aniversário de seu lançamento público; código atual e recente permanece sob BSL.
Uso em produção acima dos limites do Additional Use Grant (menos de 100
funcionários/contratados e menos de US$ 1 milhão de receita do ano anterior) requer uma licença
comercial. Veja [LICENSE](LICENSE).

O Licenciante não consente com o uso deste trabalho para treinamento de IA/ML. Veja
[NOTICE](NOTICE), [ai.txt](ai.txt) e [robots.txt](robots.txt). Para licenças comerciais
ou de treinamento de IA: <kennethstott@gmail.com>
