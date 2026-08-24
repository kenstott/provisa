# Provisa

**Conecte seus bancos de dados. Consulte com GraphQL, gRPC, SQL ou MCP — sobre qualquer API ou protocolo — em 5 minutos.**

O Provisa serve toda superfície de API (REST, GraphQL, SQL, gRPC, MCP e mais) sobre o resultado unido de suas fontes. Ele consegue fazer isso porque é uma **camada semântica ativa**: uma única definição do seu patrimônio de dados — cada domínio, relacionamento e política em todas as suas fontes, excluindo apenas os próprios sistemas de origem — que ao mesmo tempo opera o patrimônio e o governa. A definição não é documentação que um motor pode consultar; ela *é* o motor. Domínios e relacionamentos registrados são os únicos caminhos de join legais, e políticas de acesso são compiladas em cada plano de consulta. Um modelo, três funções:

- **Definir** — Domínios, colunas e relacionamentos são declarados uma única vez. Essa declaração é o esquema que todo consumidor vê e o único conjunto de caminhos de join que qualquer consulta pode percorrer.
- **Aplicar** — Segurança em nível de linha, mascaramento de coluna, visibilidade de coluna e aprovação de consulta são aplicados inline no caminho de execução. Nenhuma consulta chega aos dados sem passar por eles, então a cobertura é total por construção, não por diligência.
- **Auditar** — Como toda requisição percorre o mesmo caminho governado, quem consultou o quê, sob qual função e contra qual política é registrado de forma uniforme. Traces distribuídos, métricas e logs são eles mesmos registrados como tabelas consultáveis ao lado dos seus dados de negócio.

Um único núcleo governado serve toda linguagem e transporte. Consulte com **GraphQL, Cypher ou SQL**; consuma via **pgwire, Bolt, gRPC, REST, Arrow Flight ou JDBC**. Cada linguagem de consulta é reduzida a uma única representação intermediária onde a governança é injetada uma vez — de modo que uma política não possa divergir entre linguagens — e essa IR é retargetada para o dialeto nativo de cada fonte na saída. Adicionar uma linguagem é um novo front-end sobre o núcleo compartilhado, não um novo motor.

O patrimônio é ao mesmo tempo analítico e transacional. Leituras entre fontes se espalham pela camada de federação; escritas e leituras de fonte única são roteadas diretamente ao driver da fonte — governadas de forma idêntica, mas transacionais e com sub-100ms. Streaming colunar Arrow Flight já vem embutido.

Todo o modelo é construído a partir de um punhado de primitivas — domínios, relacionamentos, funções e políticas. Vocabulário pequeno, então a definição é fácil de compreender e simples de avaliar e auditar: você pode ler o conjunto de políticas e saber o que ele faz. O Provisa é um compilador de consultas leve, não um runtime que fica no caminho dos dados. Ele converte uma requisição em consultas nativas, as roteia, e sai do caminho — por isso o patrimônio tem desempenho.

Esse design suporta duas formas de uso, e elas não são exclusivas:

- **Como andaime para modernização** — Modele seu patrimônio, deixe o Provisa gerar o SQL nativo para cada fonte, então capture esse SQL e adote-o diretamente no sistema de destino. O Provisa é a camada de transição, não uma dependência permanente.
- **Como infraestrutura permanente de aplicação de políticas** — Mantenha-o em vigor como o caminho governado que toda consulta percorre, para que definição, aplicação e auditoria permaneçam unificadas enquanto o patrimônio existir.

## O modelo de federação

Todo o modelo se resume a dois contratos e duas políticas: fontes se reduzem a tabelas 2-D sobre um sistema de tipos, consultas se reduzem a uma IR estilo SQL, a alcançabilidade decide o que é consultado ao vivo versus materializado, e uma estratégia de atualidade governa cada cópia materializada e conjunto de dados derivado. Formato de dados na entrada, formato de consulta na entrada, governança no join, consultas nativas na saída. O restante desta seção percorre cada peça.

O modelo repousa sobre uma única redução: toda fonte é expressa como uma coleção de tabelas bidimensionais sobre um único sistema de tipos generalizado. Esse é o contrato que uma fonte deve cumprir para se juntar ao patrimônio, e é o mesmo contrato para todas elas. Algumas fontes já se encaixam — uma tabela MySQL ou PostgreSQL *é* uma relação 2-D tipada. Algumas se encaixam com uma projeção: um resultado de consulta GraphQL, uma vez achatado, é uma tabela. Algumas são estranhas ao formato — triplestores SPARQL, Neo4j — mas permanecem viáveis, porque o usuário fornece uma consulta cujo conjunto de resultados é tabular; a consulta é o adaptador. Seja qual for a fonte, o patrimônio enxerga linhas, colunas e tipos generalizados, e nada mais. Integrar um novo tipo de fonte é cumprir esse único contrato, às vezes com uma etapa de intervenção humana, não escrever uma integração sob medida.

Essa redução tem uma gêmea do lado da consulta. SQL — em todos os seus dialetos e peculiaridades — é essencialmente a linguagem para análise sobre conjuntos de dados 2-D, o que torna uma forma estilo SQL o alvo universal natural para consultas. Então toda requisição, em qualquer linguagem que chegue, é reduzida a essa representação intermediária como seu primeiríssimo passo. Algumas se reduzem de forma limpa — o próprio SQL, e até GraphQL; algumas são difíceis — a semântica de caminhos e grafos do Cypher exige trabalho real — mas todas são viáveis. Canalizar toda requisição para uma única IR antes de qualquer outra coisa acontecer é o que permite que a governança se aplique em exatamente um lugar, sobre uma forma, independentemente da linguagem em que chegou.

Sobre essas duas formas uniformes — fontes tabulares e uma única forma de consulta — federação aqui significa tanto consulta ao vivo quanto data warehousing — o mesmo escopo que um motor de consulta ao vivo como o Trino cobre, mais a materialização em que tais motores se apoiam. O conceito que os unifica é a **alcançabilidade**: para qualquer fonte, o motor pode consultá-la no lugar, ou seus dados precisam primeiro ser materializados em algum lugar consultável? A alcançabilidade particiona o patrimônio entre o que é consultado ao vivo e o que é copiado primeiro.

A maioria dos bancos de dados já carrega alguma noção de link ao vivo — `ATTACH` do DuckDB, `postgres_fdw` do PostgreSQL, links externos do Databricks. Então a maioria dos bancos de dados pode atuar como um motor de federação em algum grau. Nenhum é abrangente: cada um alcança um conjunto particular de fontes e materializa o resto, sem um relato único de qual é qual. O modelo fecha essa lacuna tornando a alcançabilidade explícita — um conjunto definido de métodos, por fonte, que declara o que o motor consegue alcançar ao vivo e, por eliminação, o que precisa ser materializado.

O que resta é a atualidade: para cada fonte não alcançável, quão atual precisa estar sua cópia materializada? Na prática, isso se reduz a um pequeno conjunto de estratégias — sob demanda, em uma programação, sob um sinal de mudança (CDC, watermark, snapshot), ou fixada. Escolher uma por fonte é toda a política de atualidade.

Conjuntos de dados analíticos — tabelas derivadas, agregados, as saídas de uma transformação — se encaixam na mesma forma. Eles também precisam ser expressos na IR, e por serem, a linhagem não é um sistema separado a manter: o caminho de cada sistema de origem até uma saída final *é* a IR que a produziu, legível de ponta a ponta. Construí-los levanta a questão de atualidade um passo além — o conjunto de dados é atualizado em uma programação, apenas quando suas pré-condições são atendidas, continuamente em quase tempo real, ou como um snapshot histórico fixado? As formas de expressar como e quando construir um conjunto de dados são o mesmo pequeno conjunto enumerável, então um conjunto de dados derivado carrega uma política de construção exatamente no mesmo vocabulário que uma cópia de fonte.

Modelos dimensionais são uma aplicação direta. As tabelas fato e dimensão de um esquema estrela são conjuntos de dados analíticos como qualquer outro — uma dimensão é uma projeção conformada e deduplicada; uma tabela fato é um join e agregado reduzido a uma granularidade — cada uma carregando sua própria política de construção e atualidade. Dimensões de mudança lenta não precisam de maquinário especial: um snapshot fixado é histórico Tipo 2, uma reconstrução programada é Tipo 1. E como o esquema é definido na IR em vez de fisicamente vinculado às tabelas de um único warehouse, as mesmas definições de fato e dimensão são retargetadas — materializadas no Oracle, no Databricks, ou deixadas virtuais sobre um motor MPP — sem remodelagem. O modelo gera o esquema estrela; ele não o prende a um motor.

O Data Vault se encaixa da mesma forma, uma camada antes. Seus hubs são conjuntos de dados de chave de negócio deduplicados, seus links são os relacionamentos registrados entre eles, e seus satellites são conjuntos de dados de atributos com carimbo de tempo e apenas inserção — o registro histórico. Um satellite é apenas um conjunto de dados derivado na estratégia de atualidade por sinal de mudança: data de carga mais hashdiff é CDC aplicado a atributos descritivos, e histórico apenas-inserção é a estratégia de snapshot fixado. Tabelas point-in-time e bridge são conjuntos de dados derivados adicionais construídos para desempenho de consulta. Então um raw vault é um conjunto de conjuntos de dados analíticos na IR, e um esquema estrela é uma projeção sobre ele — ambos gerados, ambos portáveis entre motores. O que o modelo não faz é decidir a metodologia: o que se torna um hub, a granularidade de um satellite, a estratégia de divisão. Essas permanecem escolhas de modelagem; uma vez feitas, vivem como IR portátil em vez de ETL soldado a um único warehouse.

Ambos os padrões são declarados através de **dois atalhos de primeira classe** em vez de views escritas à mão — as primitivas a partir das quais todo esquema estrela e Data Vault são construídos, mantidas neutras quanto à metodologia:

- **`entity`** — uma projeção com chave, deduplicada e opcionalmente historizada de uma fonte. Declare uma chave de entidade, os atributos e um modo de histórico; o Provisa a reduz a uma view materializada, e quando histórico é solicitado, a uma **MV bitemporal** (`scd2` → delta, `snapshot` → snapshot). Uma construção serve tanto a uma **dimensão** Kimball (SCD1/SCD2) quanto a um **hub + satellite** de Data Vault.
- **`fact`** — um join a chaves de entidade, reduzido a uma granularidade declarada, com medidas agregadas. O Provisa a reduz a uma MV agregada mais relacionamentos registrados às entidades. Uma construção serve tanto a uma **tabela fato** estrela quanto a um **link** de Data Vault (um fato sem medidas é um link puro de conjunto de chaves).

Como a redução é pura — uma especificação de `entity`/`fact` se torna exatamente as definições de MV, bitemporal e relacionamento que um modelador escreveria à mão — o warehouse é IR do início ao fim e é retargetado entre motores sem remodelagem. Declare um warehouse na UI de administração (um formulário de **Model** para entidades e fatos) ou via a API de administração (`registerEntity` / `registerFact`); o modelo *gera* o esquema estrela Kimball ou o Data Vault, ele não impõe um.

### Viagem no tempo

Viagem no tempo é uma ideia simples — manter toda versão de uma linha em vez de sobrescrevê-la, para que você possa perguntar o que os dados *eram* em qualquer momento passado. O que difere é a eficiência com que cada motor consegue fazer isso, e é exatamente por isso que o Provisa a torna uma propriedade da **definição** da view materializada em vez do motor de armazenamento (REQ-1162). Declare uma vez; funciona em qualquer backend que materialize.

A regra que a mantém portátil é **apenas-anexação**: uma versão, uma vez escrita, nunca é atualizada ou excluída. Aposentar uma linha escrevendo de volta uma data "válido-até" — o truque bitemporal usual — precisa de um UPDATE, que muitos motores não conseguem fazer de forma barata (ou de forma alguma) sobre um armazenamento federado, então o Provisa não o faz. Em vez disso, toda atualização **anexa**, e "qual versão estava em vigor no momento T" é derivado no momento da leitura a partir do log imutável. Existem exatamente duas formas de anexar:

- **Snapshot** — anexa todo o conjunto de dados atualizado, carimbado com o tempo de sistema desta atualização. Sem diffing; correto em qualquer motor; o armazenamento cresce uma cópia completa por atualização.
- **Delta** — anexa apenas o que mudou, mais tombstones para chaves removidas. O delta é **calculado pelo motor** (anti-joins dentro de um `INSERT … SELECT`), nunca dobrado linha a linha no Provisa. Menor, e precisa de uma chave de entidade.

O tempo de sistema (quando o Provisa registrou uma versão) é gerenciado dessa forma; o tempo válido (quando um fato é verdadeiro no negócio) é fornecido pelo próprio SELECT da view e preservado. Motores que oferecem mais — snapshots nativos do Iceberg, um MERGE que mantém menos linhas — podem ser visados para eficiência atrás da mesma declaração; o caminho apenas-anexação é o piso que é correto em todo lugar.

A leitura é transparente. Uma consulta simples contra uma MV bitemporal reconstrói o estado **atual** a partir do log de anexação por padrão; para viajar no tempo, envie um cabeçalho `X-Provisa-As-Of: <timestamp>` e toda a consulta é respondida como o patrimônio estava naquele momento — semântica idêntica em qualquer substrato. Ative-a para qualquer view materializada na UI de administração (um controle de **Time Travel**: desligado / snapshot / delta mais uma chave de entidade) ou via a API de administração.

Alcançabilidade mais atualidade é um modelo geral para federação de dados: uma definição que diz o que está ao vivo, o que está materializado, e quão atual cada cópia permanece — independente do alcance de qualquer motor específico. O resultado é liberdade de aprisionamento proprietário. O modelo é portátil; o patrimônio não é cativo de qualquer fornecedor cuja federação hoje alcance mais fontes.

## Recursos

### Interfaces de consulta

Estas são as linguagens e APIs estruturadas em que você escreve consultas. Cada uma tem sua própria sintaxe e semântica; a governança (RLS, mascaramento, visibilidade de coluna, aplicação de relacionamento) se aplica uniformemente a todas elas, independentemente de qual protocolo de rede as entrega.

- **GraphQL** — Esquemas por função com visibilidade em nível de campo, filtragem, paginação baseada em cursor e consultas agregadas (`count`, `sum`, `avg`, `min`, `max`). Restrito por esquema aos relacionamentos registrados — estruturalmente válido por construção, o caminho mais rápido para uma consulta simples correta. Apollo APQ incluído: consultas são hasheadas e registradas no servidor; chamadas subsequentes enviam apenas o hash via HTTP GET, tornando respostas cacheáveis por CDN sem exigir mudanças no cliente. Tabelas de consulta abaixo de um limite configurável de linhas são expostas como tipos enum.
- **SQL** — SQL completo sobre dados federados; irrestrito e mais expressivo que GraphQL. Escreva SQL padrão — subconsultas correlacionadas e tudo mais — e ele roda entre fontes sem alterações. Consultas de fonte única contornam a camada de federação completamente (sub-100ms).
- **Cypher** — Linguagem de consulta a grafos sobre o mesmo esquema federado. Percorra relacionamentos como arestas de grafo; una fontes; caminhos de comprimento variável. A governança se aplica de forma idêntica a GraphQL e SQL.
- **API de modelo gRPC** — `.proto` gerado automaticamente a partir do esquema registrado; RPCs de consulta e inserção tipadas por tabela, respostas em streaming. Orientado a esquema no mesmo sentido que GraphQL — o modelo de registro é o contrato, protobuf é a codificação de rede. Diferente do Arrow Flight (que é um transporte de streaming colunar), esta é uma interface de consulta completa por tabela.
- **JSON:API** — API de consulta estruturada em `/data/jsonapi/{table}`, apenas HTTP por design. Suporta JSON:API 1.1: conjuntos de campos esparsos (`fields[table]=col1,col2`), expressões de filtro (`filter[field][op]=value`), documentos compostos (`include=relation`) e ordenação. Não é uma linguagem de consulta de propósito geral — consulta uma tabela por vez com sintaxe de filtro padronizada em vez de uma string de consulta ad-hoc.
- **Explorador de linguagem de consulta** — Escreva uma consulta GraphQL e veja traduções ao vivo de **SQL Semântico** e **Cypher** em painéis laterais; copie qualquer uma delas ou vá direto ao editor de SQL ou de Grafo. Um fluxo de trabalho prático é esboçar fragmentos de consulta em GraphQL, depois costurar o SQL resultante em views ou relatórios complexos.

O Explorer mostra uma consulta GraphQL ao lado de suas traduções ao vivo em SQL e Cypher:

![Query Language Explorer](docs/images/query-explorer.png)

O mesmo esquema federado é explorável como um grafo ao vivo — rótulos de domínio e nó, tipos de relacionamento e travessias de comprimento variável:

![Graph Visualization](docs/images/graph-view.png)

### Ferramentas de composição de consulta

Estas ferramentas ajudam você a escrever consultas nas linguagens acima — elas não são linguagens de consulta em si.

- **Consulta em linguagem natural** — Pipeline NL→SQL/Cypher/GraphQL alimentado pelo Claude. Descreva o que você quer em português simples; o pipeline produz uma consulta na linguagem escolhida com um laço de validação interativo antes da execução.

![Natural Language Query](docs/images/natural-language.png)

### Protocolos de rede

Estes são os protocolos de conexão. SQL, GraphQL e Cypher trafegam sobre eles — a escolha do protocolo de rede não altera a interface de consulta nem o comportamento de governança.

- **pgwire** — Qualquer cliente PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` do pandas) conecta na porta 5439 como se fosse um servidor Postgres. Aceita apenas SQL. O pipeline completo de governança se aplica. `pg_catalog` e `information_schema` são respondidos a partir de um catálogo em memória, então navegadores de esquema funcionam sem uma ida e volta de federação. TLS opcional.
- **Bolt (Neo4j)** — Qualquer cliente Neo4j (Neo4j Browser, Bloom, drivers oficiais) conecta via protocolo Bolt e executa Cypher contra o grafo federado. Cada função que o usuário possui aparece como um banco de dados `provisa_<role>`. Mesma governança que qualquer outro transporte. TLS opcional.
- **Arrow Flight** — Streaming colunar de alta vazão sobre gRPC; aceita GraphQL ou SQL como entrada de consulta. Conjuntos de resultados ilimitados, sem materialização no servidor, sem infraestrutura separada necessária.
- **JDBC** — Integração com ferramentas de BI (Tableau, Power BI, DBeaver) em modo `approved` ou `catalog`.
- **WebSocket / SSE** — Subscriptions: eventos de mudança quase em tempo real; backends: PG nativo, MongoDB nativo, CDC, polling. Também exposto via Kafka.

### Fontes de dados

- **53 tipos de fonte** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets e mais através de uma única API; fontes de grafo e RDF são de primeira classe, não adaptadores
- **Roteamento inteligente** — Consultas de fonte única contornam a federação (sub-100ms); consultas de múltiplas fontes são roteadas pela camada de federação — traga seu próprio cluster ou use os workers embutidos
- **Fontes de API** — Registre endpoints REST, GraphQL, gRPC, WebSocket ou RSS como tabelas consultáveis; helpers SPARQL incluídos; joins federados entre fontes de API e fontes relacionais funcionam de forma transparente
- **Introspecção de esquema remoto** — Aponte para qualquer endpoint GraphQL, OpenAPI ou gRPC; operações documentadas são automaticamente expostas como tabelas consultáveis, nós de grafo e arestas com governança completa aplicada por cima
- **Fontes de arquivo** — Arquivos CSV, Parquet e SQLite como tabelas consultáveis; suporta caminhos locais e armazenamento de objetos remoto (`s3://`, `ftp://`, `sftp://`)
- **Integração Kafka** — Tópicos como tabelas somente-leitura; resultados de consulta como sinks Kafka
- **Gatilhos programados** — Gatilhos cron e por intervalo (APScheduler) que disparam webhooks, mutações ou publicações em sinks Kafka
- **Dicas de desempenho de federação** — Dicas de roteamento via comentário SQL sobrepõem decisões automáticas de roteamento

![Data Sources](docs/images/data-sources.png)

Fontes, arquivos e endpoints remotos são registrados como tabelas governadas a partir da UI:

![Table Registration](docs/images/table-registration.png)

### Segurança e Governança

- **Segurança em nível de linha** — Injeção de cláusula WHERE por tabela, por função
- **Mascaramento de coluna** — Mascaramento por coluna (regex, constante, truncamento) com contorno baseado em função
- **Presets de coluna** — Valores estáticos ou de variável de sessão do lado do servidor, injetados em insert/update; não expostos nos tipos de entrada de mutação
- **Permissões de escrita** — Controle de acesso de mutação por coluna (`writable_by`)
- **Funções herdadas** — Funções herdam RLS, visibilidade e mascaramento de uma função pai recursivamente
- **Funções e webhooks rastreados** — Funções de BD e webhooks de saída expostos como mutações GraphQL com formas de retorno tipadas
- **Hook de aprovação ABAC** — Hook de autorização pré-execução; transporte webhook, gRPC ou unix_socket; escopo por tabela, por fonte ou global; política de fallback configurável
- **Autenticação plugável** — Firebase, Keycloak, OAuth 2.0, simples (testes)

![Security Roles](docs/images/security-roles.png)

### Entrega e desempenho

- **Views materializadas como transformações registradas** — Uma MV captura a transformação que a produziu: sua forma de join ou SQL, os sinais de entrada por fonte (snapshot do Iceberg, watermark de RDB) a partir dos quais foi construída, e uma verificação de determinismo no registro. Como a transformação é registrada, consultas (ou subexpressões) são reescritas de forma transparente sobre uma MV atualizada — correspondência estrutural de padrão de join com suporte a correspondência parcial, então uma MV cobrindo um subconjunto dos joins ainda se aplica, com os joins restantes preservados
- **Inlining de tabelas quentes** — Tabelas de consulta pequenas e frequentemente unidas são embutidas como CTEs VALUES diretamente no plano de consulta, eliminando idas e voltas entre fontes para dados de dimensão
- **Cache de consulta** — Cache de resultado Redis particionado por função+RLS; cache de hash APQ incluído
- **Observabilidade como dado** — Traces distribuídos, métricas e logs são coletados via OpenTelemetry, compactados em Iceberg no S3, e automaticamente registrados como tabelas consultáveis (`traces`, `metrics`, `logs`, `queries`) no esquema federado; consulte-os com SQL, GraphQL ou Cypher ao lado dos seus dados de negócio — junte uma tabela `customers` à tabela `queries` para ver quem executou o quê e quanto tempo levou

### Administração e integração

- **API de administração** — GraphQL em `/admin/graphql`; upload/download de configuração, edição de relacionamento, aprovação de consulta
- **Visualizador de relatórios** — `/admin/reports` lista as visões de gestão integradas do domínio de operações e quaisquer relatórios personalizados registrados; exige a capacidade `observability`
- **Prévia de tabela** — toda tabela registrada tem um visualizador de dados governado com paginação no servidor, filtros empurrados para a fonte, agrupamento em múltiplos níveis e exportação CSV
- **GraphQL Voyager** — Visualização interativa de esquema, com escopo por função, como diagrama entidade-relacionamento
- **Descoberta de relacionamento por LLM** — Sugestões de candidatos a chave estrangeira alimentadas pelo Claude
- **Cliente Python** — `pip install provisa-client`; GraphQL/SQL → DataFrames, Arrow Flight → Tables pyarrow, dialeto SQLAlchemy, suporte a ADBC
- **Ingestão de dados** — Endpoints HTTP para enviar dados de evento JSON para a plataforma
- **Importação Hasura v2 / DDN** — Converta metadados Hasura v2 ou YAML de supergraph DDN em configuração Provisa
- **Apollo Federation** — Exponha o Provisa como um subgraph Apollo Federation v2

Esquema com escopo por função visualizado como diagrama entidade-relacionamento (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Relacionamentos são registrados, aprovados e aplicados como os únicos caminhos JOIN legais:

![Relationships](docs/images/relationships.png)

## Modelo de segurança

É aqui que "no caminho que toda consulta já percorre" deixa de ser um slogan. O Provisa aplica um modelo de segurança em múltiplas camadas em toda linguagem de consulta (GraphQL, SQL, Cypher) e todo transporte (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). A governança é aplicada de forma uniforme — não há caminho de consulta que a contorne. A cobertura é total por construção, não por diligência: adicione uma fonte, coluna ou relacionamento e toda camada se aplica a ela automaticamente, sem nada para lembrar de registrar.

As camadas se aplicam em ordem. Uma requisição precisa passar por cada camada antes que a próxima seja avaliada.

### Camada 0 — Filtragem de introspecção

O esquema e o catálogo apresentados a uma função contêm apenas as tabelas em sua lista `domain_access` e as colunas que passam pelas regras `visible_to` por coluna. Objetos fora do acesso de uma função são invisíveis no momento da descoberta — não podem ser consultados, autocompletados ou inferidos como existentes. Isso se aplica ao esquema GraphQL, ao catálogo SQL e ao navegador de esquema do editor de consultas.

### Camada 1 — Acesso público

Tabelas em domínios sem restrição de `domain_access` são visíveis a todas as identidades autenticadas sem configuração adicional. Fricção zero para dados genuinamente públicos.

### Camada 2 — Acesso a domínio

Cada função carrega uma lista `domain_access` de IDs de domínio. Uma consulta que toca uma tabela fora desses domínios é rejeitada antes da execução. Esta é a fronteira grosseira de propriedade — uma função de RH não pode alcançar tabelas financeiras independentemente de como o SQL é escrito.

### Camada 3 — Segurança em nível de linha

Depois que o acesso a domínio é confirmado, predicados `WHERE` por tabela, por função são injetados em todo `SELECT` no momento da execução. Os predicados avaliam contra os dados brutos. Um gerente regional consultando uma tabela de pedidos compartilhada vê apenas as linhas de sua região mesmo em um `SELECT *`.

### Camada 4 — Visibilidade e mascaramento de coluna

Colunas com uma lista `visible_to` que exclui a função solicitante são removidas da saída da consulta. Colunas com uma regra de mascaramento têm seus valores substituídos — redação por regex, substituição constante ou truncamento — antes que os resultados deixem o servidor. O mascaramento se aplica em todas as linguagens de consulta e formatos de saída.

### Camada 5 — Guarda de predicado

Colunas mascaradas são rejeitadas em cláusulas `WHERE` e `HAVING`. Sem isso, um chamador poderia inferir o valor não mascarado fazendo busca binária dele em um filtro mesmo que a saída esteja mascarada. A rejeição é aplicada no momento da análise da consulta, antes da execução.

### Governança de relacionamento

Condições JOIN em SQL precisam corresponder a um relacionamento registrado e aprovado entre tabelas. Joins não aprovados são rejeitados. Cada relacionamento carrega um motivo e descrição legíveis por humanos — orientação tanto para usuários quanto para agentes autônomos sobre por que um caminho de travessia existe. Isso é política de governança, não uma fronteira de segurança rígida: as Camadas 2–5 se mantêm independentemente da estrutura do join, então uma burla deliberada não expõe dados que a função não poderia alcançar através de duas consultas separadas. Tentativas de burla são registradas em log e auditáveis.

---

Essas camadas se compõem. Uma função com acesso a domínio, RLS e colunas mascaradas tem todas as cinco restrições ativas simultaneamente. Adicionar uma nova fonte de dados, coluna ou relacionamento não exige atualizar cada regra — cada camada é configurada de forma independente e se aplica automaticamente a qualquer consulta que toque objetos governados.

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
2. Torne-o executável e execute-o — a primeira execução completa uma configuração única (sem internet necessária):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Baixe [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (sempre a versão mais recente)
2. Execute o instalador — não requer privilégios de administrador
3. Abra **Provisa First Launch** no Menu Iniciar — completa uma configuração única (~5 min, sem internet necessária)
4. Abra um novo terminal:

```bash
provisa start
```

### Primeira consulta

Em desenvolvimento local (`PROVISA_MODE=test`), nenhuma credencial é necessária. Em produção, autentique-se com um token Bearer — a função é extraída dele automaticamente.

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

Autentique-se com seu usuário e senha do Provisa — o servidor atribui sua função.

- **modo `catalog`** — esquema completo visível; use com ferramentas de catálogo (Collibra, Atlan, DBeaver)

Veja [docs/integrations.md](docs/integrations.md) para os passos de configuração do Tableau e Power BI.

### Protocolo de rede PostgreSQL (pgwire)

O Provisa fala o protocolo de rede PostgreSQL na porta 5439. Qualquer cliente que consiga conectar ao Postgres conecta ao Provisa — sem driver, sem adaptador, sem mudanças nas ferramentas existentes.

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

Todas as consultas passam pelo pipeline completo de governança — acesso a domínio, RLS, mascaramento e guarda de predicado se aplicam exatamente como fazem para GraphQL e REST. Navegadores de esquema (DBeaver, DataGrip, pgAdmin) funcionam prontos para uso: consultas a `pg_catalog` e `information_schema` são respondidas a partir de um catálogo em memória com escopo no acesso a domínio da função, então os usuários veem apenas as tabelas e colunas que têm permissão para consultar.

DataGrip navegando o esquema governado e seu diagrama de chave estrangeira via pgwire — sem driver, sem adaptador:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

O TLS é habilitado configurando `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY`. A porta é configurável via `PROVISA_PGWIRE_PORT` (padrão `5439`).

### Bolt (protocolo de rede Neo4j)

O Provisa também fala o protocolo **Bolt** do Neo4j, então ferramentas nativas de grafo conectam diretamente e executam Cypher contra o grafo federado — sem exportação, sem banco de dados de grafo separado. Aponte o **Neo4j Browser** ou o **Bloom** para o Provisa e percorra relacionamentos entre fontes com a mesma governança (acesso a domínio, RLS, mascaramento) aplicada.

Neo4j Browser executando Cypher contra o Provisa — rótulos de nó, tipos de relacionamento e chaves de propriedade vêm direto do esquema registrado:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Habilite-o configurando `PROVISA_BOLT_PORT` (o padrão do Neo4j é `7687`). O TLS é habilitado com `PROVISA_BOLT_CERT` e `PROVISA_BOLT_KEY`. Cada função Provisa que o usuário autenticado possui aparece como um banco de dados selecionável `provisa_<role>` (o seletor `provisa_admin` acima) — escolher um restringe a sessão aos direitos de domínio dessa função; o usuário nunca pode exceder as funções que possui.

### Cliente Python

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

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Veja [docs/python-client.md](docs/python-client.md) para a referência completa.

## Documentação

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
| Tipos de fonte suportados | [docs/sources.md](docs/sources.md) |
| Subscriptions SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, ferramentas de BI, clientes Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Cliente Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API de administração | [docs/admin.md](docs/admin.md) |
| Implantação (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Importação Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Fluxo de trabalho de release (tags alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamento

O Provisa inclui um motor de federação embutido para consultas de múltiplas fontes. Na primeira execução você escolhe um orçamento de RAM; o Provisa deriva o número de workers de federação locais automaticamente.

| RAM do host | Workers | Carga de trabalho típica |
| --- | --- | --- |
| < 24 GB | 0 | Desenvolvimento, consultas de fonte única, equipes pequenas |
| 24–47 GB | 1 | Equipe pequena, consultas moderadas entre fontes |
| 48–95 GB | 2 | Implantação departamental, uso misto de BI + notebook |
| 96 GB+ | 4 | Departamento grande, federação concorrente pesada |

O número de workers pode ser alterado a qualquer momento editando `~/.provisa/config.yaml` (`federation_workers: N`) e executando `provisa restart`. Defina como `0` para rodar apenas coordenação (nó único).

### Escalando além de uma única máquina

**Escalonamento horizontal** — Execute múltiplas instâncias do Provisa atrás de um balanceador de carga. Cada instância é um sistema totalmente funcional. Todas as instâncias precisam apontar para o mesmo BD de configuração (defina `CONFIG_DB_HOST` nas máquinas secundárias) e, opcionalmente, uma instância Redis compartilhada (`REDIS_URL`) para um cache unificado. A maioria das consultas se distribui de forma transparente; joins entre fontes muito grandes podem exceder os recursos de uma única instância e exigir uma máquina maior ou um cluster de federação externo.

**Redis compartilhado** — Defina `REDIS_URL` em cada instância para apontar para um Redis externo. Redis compartilhado significa que entradas de cache de uma instância ficam disponíveis para todas, melhorando as taxas de acerto em todo o cluster.

**Traga seu próprio cluster de federação** — Aponte o Provisa para um cluster de federação externo existente em vez dos workers embutidos. Recomendado para implantações de grande escala ou em nuvem; veja [docs/deployment.md](docs/deployment.md) para configuração.

## Licença

Business Source License 1.1 (sem modificações, conforme os pactos de Licenciante da MariaDB). Cada
versão lançada converte-se para a Change License (GPL v2.0 ou posterior) no 4º
aniversário de seu lançamento público; código atual e recente permanece sob BSL.
Uso em produção acima dos limites do Additional Use Grant (menos de 100
funcionários/contratados e menos de US$ 1 milhão de receita do ano anterior) requer uma licença
comercial. Veja [LICENSE](LICENSE).

O Licenciante não consente com o uso deste trabalho para treinamento de IA/ML. Veja
[NOTICE](NOTICE), [ai.txt](ai.txt) e [robots.txt](robots.txt). Para licenças comerciais
ou de treinamento de IA: <kennethstott@gmail.com>
