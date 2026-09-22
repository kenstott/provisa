# Referência da API GraphQL de administração

A API GraphQL de administração é o plano de configuração do Provisa. É a API que o aplicativo web de administração chama para toda operação de gerenciamento — criar fontes, registrar tabelas, definir relacionamentos, configurar regras de RLS, e tudo mais que molda o modelo.

**Ponto de montagem:** `POST /admin/graphql`

Esta não é a mesma API do plano de dados em `/data/graphql`. O plano de dados atende consultas de usuários finais sobre domínios registrados e é descrito pelo SDL em `/data/sdl`. A API de administração configura a aparência desse esquema e quem pode ver o quê.

---

## Como a interface conversa com esta API

O aplicativo web de administração usa o Apollo Client, apontado para `${API_BASE}/admin/graphql`. [tool-verified: `provisa-ui/src/apolloClient.ts:19`]

Toda requisição carrega um token bearer (buscado novamente do provedor de autenticação a cada chamada), um cabeçalho `X-Org-Id` quando multilocatário, e um cabeçalho `X-Env` ao atender um ambiente de branch. [tool-verified: `provisa-ui/src/apolloClient.ts:24-42`]

O esquema é montado a partir de duas classes `@strawberry.type` — `Query` de `schema_query.py` e `Mutation` de `schema_mutation.py` — e envolvido em uma `ModelCommitExtension` que registra toda mutação contra o branch de ambiente atual (REQ-1524). [tool-verified: `provisa/api/admin/schema.py:44`]

---

## Autorização

**Modo de desenvolvimento:** quando nenhuma autenticação está configurada e toda requisição chega como um principal anônimo, todas as verificações de capacidade são ignoradas. Isso mantém uma instalação local funcional sem configuração de autenticação. [tool-verified: `provisa/api/admin/capabilities.py:98-99`]

**Portões de capacidade:** implantações de produção exigem capacidades nomeadas. O direito específico exigido por cada campo é anotado inline. Chamar uma mutação sem a capacidade exigida gera um `PermissionError`. A função de administrador da plataforma ignora todas as verificações de capacidade (REQ-1297). [tool-verified: `provisa/api/admin/capabilities.py:80-110`]

**Portões de domínio:** várias mutações também verificam o domínio ao qual o objeto pertence. Um chamador com escopo em `sales` não pode registrar uma tabela em `finance`, enfileirar uma regra de RLS para ela, ou criar um relacionamento cuja tabela de origem esteja em um domínio que ele não possui (REQ-1530, REQ-1531). Views são ainda mais restritas: toda tabela que o SQL da view lê deve estar dentro dos domínios do chamador, porque SQL livre de outra forma daria a um membro acesso a dados fora de seu escopo. [tool-verified: `provisa/api/admin/domain_guard.py:1-133`]

**Herança de função:** as capacidades de uma função pai são herdadas pelas funções filhas (REQ-1677). `createRole` e `deleteRole` recusam ciclos e impedem excluir uma função que tenha herdeiras.

---

## Tipo de retorno comum

A maioria das mutações retorna `MutationResult`. [tool-verified: `provisa/api/admin/types.py:1181-1188`]

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization (REQ-1350)
}
```

Quando uma mutação falha, `success` é `false` e `message` carrega o motivo em inglês. `code` é um identificador estável que a interface usa para renderizar uma mensagem localizada.

---

## Consultas

### Fontes

#### `sources → [SourceType!]!`

Todas as fontes de dados registradas. [tool-verified: `provisa/api/admin/schema_query.py:327-331`]

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl preferMaterialized
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc { bootstrapServers topicPrefix schemaRegistryUrl consumerGroupId }
  }
}
```

`passwordRef` é uma referência `${secret:NAME}` no cofre da organização — nunca a credencial literal. [tool-verified: `provisa/api/admin/types.py:105`]

#### `source(id: String!) → SourceType`

Uma única fonte por ID. Retorna `null` quando não encontrada. [tool-verified: `provisa/api/admin/schema_query.py:334-339`]

#### `availableSchemas(sourceId: String!) → [String!]!`

Esquemas visíveis em uma fonte, filtrados para excluir os internos do Provisa. Usa introspecção nativa primeiro; recorre ao catálogo do motor quando o tipo de fonte não tem um pool direto. [tool-verified: `provisa/api/admin/schema_query.py:628-667`]

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabelas em um esquema de uma fonte, com seus comentários. Para fontes OpenAPI, retorna operações GET cuja resposta é um array ou wrapper de paginação. Para fontes GraphQL, retorna campos de consulta que retornam uma lista. Para gRPC, retorna RPCs de streaming do servidor. [tool-verified: `provisa/api/admin/schema_query.py:669-731`]

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nomes de coluna para uma tabela no catálogo do motor. Para fontes govdata, usa um resolvedor separado. [tool-verified: `provisa/api/admin/schema_query.py:845-866`]

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nomes de coluna com tipos de dados, comentários, tipos de filtro nativos, e sinalizadores de chave primária. Para fontes OpenAPI, deriva o formato a partir do esquema de resposta e dos parâmetros da operação. [tool-verified: `provisa/api/admin/schema_query.py:869-876`]

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operações não-GET de uma fonte OpenAPI (POST, PUT, PATCH, DELETE). Retorna uma lista vazia para fontes que não são OpenAPI. [tool-verified: `provisa/api/admin/schema_query.py:822-843`]

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Visualize o que um crawl de conector de arquivos descobriria — arquivos, tabelas e colunas — antes de uma fonte ser criada. As configurações exclusivas de HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) são ignoradas para raízes local, S3, FTP e SFTP. (REQ-1785) [tool-verified: `provisa/api/admin/schema_query.py:733-790`]

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Retorna o alias a usar ao registrar `tableName` em `domainId` a partir de `sourceId`. Retorna um alias simples em snake-case quando não há conflito, ou um alias prefixado pela fonte (`sqlite_b_orders`) quando o nome efetivo já está tomado por uma fonte diferente no mesmo domínio. [tool-verified: `provisa/api/admin/schema_query.py:879-922`]

---

### Tabelas

#### `tables → [RegisteredTableType!]!`

Todas as tabelas registradas, cada uma com sua lista completa de colunas. A visibilidade de colunas na resposta respeita a capacidade `table_registration` do chamador — `canDeployToDb` é condicionado a ele possuir esse direito. (REQ-016, REQ-021, REQ-042) [tool-verified: `provisa/api/admin/schema_query.py:502-536`]

Cada `RegisteredTableType` expõe subcampos computados:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — a política efetiva de atualização/serviço como texto simples, derivada no servidor a partir da mesma resolução do planejador que o motor usa. Retorna `null` durante a inicialização. (REQ-1143) [tool-verified: `provisa/api/admin/types.py:319-327`]
- **`graphqlFieldName → String`** — o nome do campo que esta tabela tem no esquema compilado do plano de dados, de modo que o painel de Produto de Dados possa montar um exemplo executável sem replicar o algoritmo de nomenclatura. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:330-339`]
- **`dqDataset → String`** — esta tabela como um conjunto de dados de contrato de qualidade de dados, na forma que o verificador varre. (REQ-1443) [tool-verified: `provisa/api/admin/types.py:374-387`]
- **`productId → String`** — o produto de dados ao qual esta tabela pertence. Uma tabela verificadora de DQ herda o produto da tabela cujo contrato ela varre. (REQ-1634) [tool-verified: `provisa/api/admin/types.py:342-372`]

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Visualize o resumo efetivo de atualização/serviço para controles de tabela em *rascunho* (não salvos), de modo que o resumo no topo do formulário se atualize conforme os campos mudam sem persistir nada. Mesma derivação de `refreshPolicySummary` acima. (REQ-1143) [tool-verified: `provisa/api/admin/schema_query.py:947-979`]

Argumentos: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `preferMaterialized`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefatos que uma renomeação de alias ou exclusão de coluna pendente quebraria. Consultivo — a interface de administração mostra isso antes de salvar e o administrador decide. Deve ser chamado *antes* de salvar, porque os dependentes foram escritos contra o nome exposto que a coluna carrega atualmente. (REQ-1484) [tool-verified: `provisa/api/admin/schema_query.py:1301-1331`]

---

### Relacionamentos

#### `relationships → [RelationshipType!]!`

Todos os relacionamentos definidos pelo usuário (exclui entradas `gql_auto__` geradas automaticamente e as entradas sintéticas `meta:%` usadas pelo ERD). [tool-verified: `provisa/api/admin/schema_query.py:539-569`]

#### `allRelationships → [RelationshipType!]!`

Igual a `relationships`, mas inclui as entradas sintéticas `meta:%`. Usado pelo ERD em grafo, que precisa mostrar toda aresta, incluindo os links implícitos `HAS_TABLE` entre tabelas de dados e o registro de metadados. [tool-verified: `provisa/api/admin/schema_query.py:572-601`]

Cada `RelationshipType` expõe:

- **`autoSuggested → Boolean`** — se o relacionamento foi sugerido pela análise de FK (o `id` começa com `fk__`). [tool-verified: `provisa/api/admin/types.py:523-525`]
- **`physicalName → String`** — o nome do relacionamento nos planos SQL e gRPC (o parâmetro `?include=`). Derivado no servidor; clientes não devem transliterar o alias GraphQL. (REQ-471, REQ-1417) [tool-verified: `provisa/api/admin/types.py:527-536`]

---

### Domínios, funções e usuários

#### `domains → [DomainType!]!`

Todos os domínios no banco de dados do locatário da organização ativa. O banco de dados do locatário é isolado em nível de esquema, portanto a lista de domínios de um administrador de organização contém apenas as linhas de sua própria organização. (REQ-021, REQ-042, REQ-1293) [tool-verified: `provisa/api/admin/schema_query.py:342-357`]

#### `roles → [RoleType!]!`

Funções visíveis ao chamador. Um administrador vê toda função; um não administrador vê apenas funções sem `org_id` ou funções pertencentes à sua organização. (REQ-042, REQ-059, REQ-060, REQ-215) [tool-verified: `provisa/api/admin/schema_query.py:603-618`]

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resolve IDs de função ou IDs de usuário para usuários individuais. Usado para expandir `DataProduct.ownerRole`, `Domain.steward` e `Column.visibleTo` em uma lista legível por humanos. Referências desconhecidas são ecoadas de volta puras, de modo que a interface mostre o ID bruto em vez de nada. [tool-verified: `provisa/api/admin/schema_query.py:388-444`]

---

### Regras RLS

#### `rlsRules → [RLSRuleType!]!`

Todas as regras de segurança em nível de linha. O repositório subjacente descriptografa `filterExpr` na borda. (REQ-041, REQ-402, REQ-686) [tool-verified: `provisa/api/admin/schema_query.py:621-625`]

---

### Produtos de dados

#### `dataProducts → [DataProductType!]!`

Todos os produtos de dados. Exige a capacidade `data_product_read`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_query.py:360-386`]

---

### Tags

#### `tags → [TagType!]!`

Todas as definições de tag, incluindo seus valores de parâmetro permitidos por tag. (REQ-1373, REQ-1467) [tool-verified: `provisa/api/admin/schema_query.py:447-475`]

#### `tagAssignments → [TagAssignmentType!]!`

Todas as atribuições de tag entre fontes, tabelas, colunas e relacionamentos. (REQ-1377) [tool-verified: `provisa/api/admin/schema_query.py:478-499`]

---

### Views materializadas

#### `mvList → [MVType!]!`

Todas as views materializadas com seu status em tempo de execução: habilitada/desabilitada, timestamp da última atualização, contagem de linhas, e último erro. [tool-verified: `provisa/api/admin/schema_query.py:927-944`]

---

### Cache

#### `cacheStats → CacheStatsType`

Estatísticas de cache. Retorna `storeType: "redis"` com métricas operacionais completas quando o Redis está configurado, `storeType: "memory"` para o armazenamento fakeredis embutido, e `storeType: "noop"` quando nenhum cache está configurado. [tool-verified: `provisa/api/admin/schema_query.py:1106-1140`]

#### `cacheTableStats → [CacheTableStatType!]!`

Contagens de entradas em cache por tabela. Vazio quando nenhum armazenamento de cache está configurado. [tool-verified: `provisa/api/admin/schema_query.py:1143-1148`]

#### `hotTables → [HotTableStatType!]!`

Tabelas das quais o Provisa mantém uma cópia, em duas camadas: `hot` (espelhada no armazenamento de resposta para inlining de JOIN) e `warm` (pousada como uma cópia Iceberg). Uma tabela está em no máximo uma camada (REQ-241). [tool-verified: `provisa/api/admin/schema_query.py:1151-1175`]

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identidade do armazenamento de materialização durável: nome do motor, referência de DSN do armazenamento, contagem de MVs, e se o armazenamento é local à instância (um arquivo local como DuckDB ou SQLite, o que significa que cada instância atrás de um balanceador de carga mantém sua própria cópia). [tool-verified: `provisa/api/admin/schema_query.py:1178-1189`]

---

### Saúde do sistema

#### `systemHealth → SystemHealthType`

Status de conexão do motor, contagens de pool de workers, estado do pool do banco de dados de metadados, modo de cache, e vivacidade de todo listener de protocolo (pgwire, gRPC, Arrow Flight, Bolt). [tool-verified: `provisa/api/admin/schema_query.py:1194-1198`]

```graphql
query {
  systemHealth {
    engineConnected engineWorkerCount engineActiveWorkers
    metadataPoolSize metadataPoolFree metadataDialect
    cacheMode cacheConnected mvRefreshLoopRunning
    protocols { name status port }
  }
}
```

---

### Tarefas agendadas

#### `scheduledTasks → [ScheduledTaskType!]!`

Gatilhos agendados a partir da configuração, com estado em tempo de execução. Cada entrada carrega sua expressão cron, `kind` (`webhook` ou `sql`), se está habilitada atualmente, o timestamp da última execução (sempre `null` nesta versão — rastreado pelo agendador), e o próximo horário de execução agendado a partir do APScheduler. [tool-verified: `provisa/api/admin/schema_query.py:1203-1245`]

---

### Qualidade de dados

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analisa texto de contrato bruto nas linhas editáveis do painel construtor. Chamado a cada edição; uma falha de análise volta como `error` em vez de um erro GraphQL, porque texto parcialmente escrito é normal enquanto o operador está digitando. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1007-1022`]

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

As verificações que `checker` oferece, restritas às colunas de `dataset`. O conjunto de dados é o alvo observado do contrato, resolvido da mesma forma que o scanner o resolve — de modo que as verificações oferecidas correspondam às colunas que o verificador realmente verá. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1025-1050`]

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

O texto de uma verificação a partir dos editores do painel. No servidor porque o dialeto tem uma única implementação; uma verificação feita pelo construtor e uma digitada manualmente devem ser indistinguíveis. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1053-1075`]

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializa linhas de verificação editadas de volta em texto de contrato. O inverso de `dqContractParse`. No servidor pelo mesmo motivo: o painel não pode emitir texto que o verificador recusaria. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_query.py:1078-1101`]

---

### Pré-visualizações de fonte

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Pré-visualize uma projeção Cypher em uma fonte Neo4j: até cinco linhas e os tipos de coluna que o registro carregará. Falhas voltam como `error`. (REQ-1670) [tool-verified: `provisa/api/admin/schema_query.py:984-992`]

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Pré-visualize um SELECT SPARQL em uma fonte SPARQL: até cinco linhas, todas as colunas como texto. (REQ-1683) [tool-verified: `provisa/api/admin/schema_query.py:995-1002`]

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Verificação em tempo real contra a API Kaggle. Retorna `true` apenas quando o token se autentica. Sustenta a etapa de portão de token no formulário de fonte Kaggle. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:793-799`]

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Busca no catálogo completo de conjuntos de dados públicos do Kaggle. Restrito por token. (REQ-1783) [tool-verified: `provisa/api/admin/schema_query.py:802-819`]

---

### Calendários

#### `calendars → [CalendarType!]!`

Todas as versões registradas de calendário de limite de snapshot. Alimenta o seletor de configuração de agenda de snapshot e confirma quais calendários uma MV periódica pode referenciar. (REQ-962) [tool-verified: `provisa/api/admin/schema_query.py:218-242`]

---

### Métricas

#### `metrics → [MetricType!]!`

Todas as definições de métrica governadas. Métricas derivadas de fato carregam `fromFact`. (REQ-1317, REQ-1320) [tool-verified: `provisa/api/admin/schema_query.py:245-264`]

---

### Versão do esquema

#### `schemaVersion → String!`

Hash SHA-256 do estado atual do esquema (domínios, IDs de tabela, IDs de relacionamento). O cliente Apollo lê isso do cabeçalho de resposta `X-Schema-Version` e refaz todas as consultas ativas quando ele avança. [tool-verified: `provisa/api/admin/schema_query.py:291-324`]

---

### Auxiliares de IA

#### `generateTableDescription(tableId: String!) → String!`

Usa o LLM configurado para gerar uma descrição de uma a duas frases para uma tabela registrada. Salve a tabela primeiro; chamar isso em uma tabela não salva retorna uma mensagem instrucional. [tool-verified: `provisa/api/admin/schema_query.py:1250-1298`]

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa o LLM configurado para gerar uma descrição de uma frase para uma única coluna. [tool-verified: `provisa/api/admin/schema_query.py:1334-1383`]

---

### Requisições de criação

#### `creationRequests → [CreationRequestType!]!`

Requisições de criação pendentes, visíveis a chamadores que possuem a capacidade de criação relevante. Usado quando um membro sem `create_relationship` ou `create_view` envia uma requisição que um detentor de direitos deve aprovar. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_query.py:267-289`]

---

## Mutações

### Fontes

#### `createSource(input: SourceInput!) → MutationResult`

Registra uma nova fonte de dados. Valida a conexão antes de persistir — uma fonte rejeitada não deixa entrada no cofre. Armazena credenciais no cofre da organização e registra a referência; o texto puro nunca chega ao banco de dados. (REQ-012, REQ-013) Exige a capacidade `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:616-781`]

#### `updateSource(input: SourceInput!) → MutationResult`

Atualiza os detalhes de conexão, descrição e configuração de uma fonte existente. Desmonta e reconecta o endpoint pgwire para fontes de arquivo/SharePoint, de modo que uma mudança de caminho tenha efeito imediato. Exige `source_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:922-1073`]

#### `deleteSource(id: String!) → MutationResult`

Remove uma fonte e sua entrada no cofre. Descarta o catálogo do motor e reconstrói os esquemas. [tool-verified: `provisa/api/admin/schema_mutation.py:1101-1132`]

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Renomeia um ID de fonte. [tool-verified: `provisa/api/admin/schema_mutation.py:1076-1098`]

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Habilita ou desabilita o cache de resultado de consulta para uma fonte, e define o TTL em segundos. [tool-verified: `provisa/api/admin/schema_mutation.py:2327-2350`]

#### `updateSourcePreferMaterialized(sourceId: String!, preferMaterialized: Boolean!) → MutationResult`

Força (ou libera) a federação materializada para todas as tabelas de uma fonte. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2379-2402`]

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marca uma fonte como protegida contra carga (somente atualização agendada). Exige pelo menos um portão — janela fora de pico, cadência de TTL de cache, ou um sinal de mudança por sondagem — ou a chamada falha. (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2431-2480`]

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Define a convenção de nomenclatura GraphQL por fonte. [tool-verified: `provisa/api/admin/schema_mutation.py:2595-2619`]

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Define quais domínios podem usar uma fonte (lista vazia = irrestrito). Exige `source_registration`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2622-2658`]

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Baixa e descompacta um conjunto de dados Kaggle no disco local. Retorna o caminho do diretório preparado; o chamador então cria uma fonte do tipo `files` apontando para ele. Pacotes contendo SQLite são rejeitados por inteiro. Exige `source_registration`. (REQ-1780–1782) [tool-verified: `provisa/api/admin/schema_mutation.py:784-824`]

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Rebusca o conjunto de dados de uma fonte derivada do Kaggle no local. Pula o download se o Kaggle não tiver nada mais recente do que o que está em disco. Exige `source_registration`. (REQ-1787) [tool-verified: `provisa/api/admin/schema_mutation.py:827-920`]

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Executa `ANALYZE` em todas as tabelas registradas de uma fonte. Melhora as decisões de ordem de junção e broadcast para consultas federadas. (REQ-276) [tool-verified: `provisa/api/admin/schema_mutation.py:2944-3008`]

---

### Tabelas

#### `registerTable(input: TableInput!) → MutationResult`

Registra uma nova tabela (ou view) em um domínio. Exige a capacidade `table_registration` e associação ao domínio alvo. Um chamador sem `create_relationship` que envia uma view é enfileirado como uma requisição de criação para um detentor de direitos aprovar. (REQ-013, REQ-016, REQ-252, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:1714-1718`]

#### `updateTable(input: TableInput!) → MutationResult`

Atualiza o alias, descrição, metadados de coluna, configurações de MV e configuração de entrega ao vivo de uma tabela existente. (REQ-016, REQ-020) Exige `table_registration`. [tool-verified: `provisa/api/admin/schema_mutation.py:1858-1995`]

#### `deleteTable(id: Int!) → MutationResult`

Exclui uma tabela registrada. Consulta o domínio da tabela para o portão de domínio antes de excluir. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1998-2029`]

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Sobrepõe o TTL de cache para uma tabela. [tool-verified: `provisa/api/admin/schema_mutation.py:2353-2376`]

#### `updateTablePreferMaterialized(tableId: Int!, preferMaterialized: Boolean) → MutationResult`

Sobrepõe a federação materializada para uma tabela. `null` = herda o padrão da fonte. (REQ-826) [tool-verified: `provisa/api/admin/schema_mutation.py:2405-2428`]

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Sobrepõe a proteção de carga para uma tabela. `null` para `loadProtected` herda o padrão da fonte. Valida a combinação efetiva de portão (tabela → fonte). (REQ-1141) [tool-verified: `provisa/api/admin/schema_mutation.py:2483-2566`]

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Define a convenção de nomenclatura GraphQL por tabela. [tool-verified: `provisa/api/admin/schema_mutation.py:2661-2685`]

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promove uma view virtual do Provisa para uma view real de banco de dados em sua fonte nativa subjacente. [tool-verified: `provisa/api/admin/schema_mutation.py:3058-3060`]

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalcula as linhas pousadas de uma tabela sob demanda, contornando o portão de mudança normal. `reason` é uma anotação de auditoria obrigatória. Recusado para tabelas federadas ao vivo (sem linhas pousadas). (REQ-968) [tool-verified: `provisa/api/admin/schema_mutation.py:2689-2782`]

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Força o próximo acesso de uma tabela de conector de arquivo SQLite a resincronizar a partir do disco. [tool-verified: `provisa/api/admin/schema_mutation.py:2877-2880`]

#### `registerEntity(input: EntityInput!) → MutationResult`

Açúcar sintático para registrar uma entidade de dimensão/hub. Rebaixa para uma MV (bitemporal, quando historizada) e chama `registerTable`. (REQ-1164) [tool-verified: `provisa/api/admin/schema_mutation.py:1721-1725`]

#### `registerFact(input: FactInput!) → MutationResult`

Açúcar sintático para registrar um fato de esquema estrela. Rebaixa para uma MV agregada, cria relacionamentos de dimensão, e registra automaticamente as medidas de fato como métricas governadas. (REQ-1164, REQ-1320) [tool-verified: `provisa/api/admin/schema_mutation.py:1728-1776`]

---

### Relacionamentos

#### `upsertRelationship(input: RelationshipInput!) → MutationResult`

Cria ou atualiza um relacionamento. O portão verifica o domínio da tabela de origem (não o do alvo). Uma aresta entre domínios é armazenada com `needsReview: true`. Um chamador sem `create_relationship` é enfileirado como uma requisição de criação. Arestas de junção (muitos-para-muitos) exigem listas de chaves de mesmo tamanho. (REQ-019, REQ-020, REQ-366, REQ-434) [tool-verified: `provisa/api/admin/schema_mutation.py:2297-2300`]

#### `deleteRelationship(id: String!) → MutationResult`

Exclui um relacionamento por ID e reconstrói os esquemas. [tool-verified: `provisa/api/admin/schema_mutation.py:2303-2322`]

---

### Domínios

#### `createDomain(input: DomainInput!) → MutationResult`

Cria um domínio. Palavras de segmento reservadas (`tables`, `relationships`, e outros segmentos de caminho URI) são rejeitadas, assim como o literal curinga `*`. Exige a capacidade `org_settings`. (REQ-021, REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1135-1189`]

#### `deleteDomain(id: String!) → MutationResult`

Exclui um domínio. Exige `org_settings`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:1192-1213`]

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Define a convenção global de nomenclatura GraphQL e reconstrói os esquemas para todas as funções. Apenas nomes de convenção reconhecidos são aceitos. (REQ-253, REQ-416) [tool-verified: `provisa/api/admin/schema_mutation.py:2571-2592`]

---

### Funções

#### `createRole(input: RoleInput!) → MutationResult`

Cria ou substitui uma função com capacidades, acesso a domínio, limites de taxa opcionais, e uma função pai opcional. Valida que a função pai existe e que a cadeia de pais é livre de ciclos. Exige `user_management`. (REQ-042, REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:1657-1712`]

#### `deleteRole(id: String!) → MutationResult`

Exclui uma função. Falha se outras funções herdam dela — reatribua o pai delas primeiro. Exige `user_management`. (REQ-1531, REQ-1677) [tool-verified: `provisa/api/admin/schema_mutation.py:2032-2063`]

---

### Regras RLS

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Cria ou atualiza uma regra de segurança em nível de linha. A expressão de filtro é validada no momento de salvar contra as colunas da tabela ou domínio alvo, de modo que uma regra que o administrador não consegue consultar seja recusada com o motivo em vez de falhar silenciosamente no momento da consulta. Exige `masking_config`. (REQ-041, REQ-402, REQ-1531, REQ-1676) [tool-verified: `provisa/api/admin/schema_mutation.py:2066-2136`]

Os alvos são mutuamente exclusivos: defina `tableId` para uma regra em nível de tabela, `domainId` para uma regra em nível de domínio, ou `actionName` para uma função/webhook rastreado. (REQ-1679)

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Exclui uma regra de RLS. Exige `masking_config`. (REQ-1531) [tool-verified: `provisa/api/admin/schema_mutation.py:2139-2178`]

---

### Produtos de dados

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Cria ou substitui um produto de dados. Exige a capacidade `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1216-1259`]

#### `deleteDataProduct(id: String!) → MutationResult`

Exclui um produto de dados. Exige `data_product_rw`. (REQ-1634) [tool-verified: `provisa/api/admin/schema_mutation.py:1262-1285`]

---

### Tags

#### `upsertTag(input: TagInput!) → MutationResult`

Cria ou atualiza uma definição de tag. Tags de sistema e tags derivadas não podem ser redefinidas. `appliesTo` deve ser um subconjunto não vazio de `["source", "table", "column", "relationship", "command"]`. (REQ-1373, REQ-1375) [tool-verified: `provisa/api/admin/schema_mutation.py:1288-1361`]

#### `deleteTag(id: String!) → MutationResult`

Exclui uma tag. Recusa tags de sistema e derivadas. (REQ-1373) [tool-verified: `provisa/api/admin/schema_mutation.py:1364-1393`]

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Atribui uma tag a uma fonte, tabela, coluna, relacionamento, ou command. Aplica as políticas de campo da tag (`reason_policy`, `expires_policy`) e — para tags parametrizadas — valida o valor do parâmetro contra a lista permitida da tag. (REQ-1376, REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1396-1527`]

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Remove uma atribuição de tag. (REQ-1377) [tool-verified: `provisa/api/admin/schema_mutation.py:1530-1564`]

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Adiciona ou redescreve um valor de parâmetro permitido para uma tag parametrizada. A lista de valores permitidos é fechada: toda atribuição deve nomear um valor dela. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1567-1616`]

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Remove um valor permitido. Recusado enquanto qualquer atribuição ainda o carregar, porque essas atribuições nomeariam um tipo que a lista não admite mais. (REQ-1467) [tool-verified: `provisa/api/admin/schema_mutation.py:1619-1655`]

---

### Métricas

#### `upsertMetric(input: MetricInput!) → MutationResult`

Cria ou substitui uma definição de métrica governada. A expressão deve ser analisável pelo sqlglot e conter ao menos uma função de agregação. Regenera todas as views compostas por métrica que referenciam esta métrica. Exige `table_registration`. (REQ-1317, REQ-1318) [tool-verified: `provisa/api/admin/schema_mutation.py:1779-1831`]

#### `deleteMetric(name: String!) → MutationResult`

Exclui uma métrica governada. Reconstrói os esquemas. Exige `table_registration`. (REQ-1317) [tool-verified: `provisa/api/admin/schema_mutation.py:1834-1856`]

---

### Calendários

#### `createCalendar(input: CalendarInput!) → MutationResult`

Cria ou substitui um calendário versionado de limite de snapshot. Validado construindo o `Calendar` em memória antes de persistir — falha em um sistema base desconhecido, fuso horário inválido, ou âncora fiscal inválida. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:525-579`]

#### `deleteCalendar(name: String!) → MutationResult`

Exclui um calendário (todas as versões). Recusado quando qualquer view materializada o referencia. (REQ-962) [tool-verified: `provisa/api/admin/schema_mutation.py:582-613`]

---

### Views materializadas

#### `refreshMv(mvId: String!) → MutationResult`

Dispara uma atualização manual de uma view materializada. Coordena entre a frota quando o modo de consistência da MV é `shared`. (REQ-133, REQ-158, REQ-879) [tool-verified: `provisa/api/admin/schema_mutation.py:2787-2813`]

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Habilita ou desabilita uma view materializada. [tool-verified: `provisa/api/admin/schema_mutation.py:2816-2839`]

---

### Cache

#### `purgeCache → MutationResult`

Purga todos os resultados de consulta em cache. [tool-verified: `provisa/api/admin/schema_mutation.py:2844-2858`]

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purga os resultados em cache de uma tabela. [tool-verified: `provisa/api/admin/schema_mutation.py:2861-2875`]

---

### Tarefas agendadas

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Cria um gatilho agendado — uma chamada de webhook ou uma instrução SQL — e o registra ao vivo no APScheduler. `kind` é `"webhook"` ou `"sql"`. (REQ-1003, REQ-1004) [tool-verified: `provisa/api/admin/schema_mutation.py:2923-2936`]

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Remove um gatilho agendado da configuração e do agendador ao vivo. (REQ-1003) [tool-verified: `provisa/api/admin/schema_mutation.py:2939-2941`]

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Habilita ou desabilita uma tarefa agendada no arquivo de configuração. [tool-verified: `provisa/api/admin/schema_mutation.py:2885-2920`]

---

### Qualidade de dados

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Executa um contrato contra a tabela ao vivo e retorna os resultados sem pousar nada. Uma mutação em vez de uma query porque custa uma varredura real. O que ela prova é se o identificador do conjunto de dados resolve para a tabela governada que o operador pretende. (REQ-1443 clause 7) [tool-verified: `provisa/api/admin/schema_mutation.py:463-487`]

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Dispara imediatamente o job de sondagem de uma tabela verificadora. Pousa linhas da forma normal, de modo que os resultados persistam e o histórico de DQ mostre a nova varredura. (REQ-1443) [tool-verified: `provisa/api/admin/schema_mutation.py:490-522`]

---

### Manutenção de esquema

#### `rebuildSchemas → MutationResult`

Reconstrói o esquema em memória a partir do estado do banco de dados. Útil após mudanças externas no banco de dados. [tool-verified: `provisa/api/admin/schema_mutation.py:456-461`]

---

### Requisições de criação

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Um detentor de direitos executa uma requisição de criação enfileirada — relacionamento, view, ou webhook. Exige a capacidade que a requisição está aguardando. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2181-2255`]

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rejeita uma requisição enfileirada com um motivo acionável. `reason` é obrigatório. (REQ-434, REQ-063) [tool-verified: `provisa/api/admin/schema_mutation.py:2258-2294`]

---

### Compilação de consulta

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila uma consulta GraphQL do plano de dados contra o esquema de uma função e retorna a decisão completa de roteamento: SQL semântico, SQL do motor, SQL direto, rota, metadados de aplicação (filtros de RLS aplicados, colunas excluídas, mascaramento aplicado), e Cypher compilado. Retorna um resultado por campo raiz na consulta. (REQ-161) [tool-verified: `provisa/api/admin/schema_mutation.py:3011-3055`]

```graphql
mutation {
  compileQuery(input: {
    role: "analyst"
    query: "{ orders { id customer_id total } }"
  }) {
    sql
    semanticSql
    engineSql
    route
    routeReason
    enforcement {
      rlsFiltersApplied
      columnsExcluded
      maskingApplied
    }
  }
}
```

Campos de `CompileQueryInput`:

| Campo | Tipo | Descrição |
|-------|------|-------------|
| `query` | `String!` | Consulta GraphQL do plano de dados a compilar |
| `role` | `String!` | Função contra cujo esquema compilar |
| `variables` | `JSON` | Vinculações de variável |
| `flatSql` | `Boolean` | Retorna uma única string SQL achatada em vez de um par semântico/motor |
| `flatCypher` | `Boolean` | Achata a saída Cypher |
| `nodeOnlyCypher` | `Boolean` | Emite Cypher somente de nós (sem padrões de aresta) |

---

## Tipos de entrada principais

### `SourceInput`

[tool-verified: `provisa/api/admin/types.py:590-611`]

| Campo | Tipo | Notas |
|-------|------|-------|
| `id` | `String!` | Identificador da fonte |
| `type` | `String!` | Tipo de conector (ex.: `postgres`, `files`, `openapi`) |
| `host` | `String` | |
| `port` | `Int` | |
| `database` | `String` | |
| `username` | `String` | |
| `password` | `String` | Texto puro ou referência `${secret:NAME}` |
| `path` | `String` | Caminho do sistema de arquivos para fontes de arquivo/CSV |
| `federationHintsJson` | `String` | Objeto JSON para extras de warehouse (warehouse/role do Snowflake, http_path do Databricks) |
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` (REQ-929) |
| `loadProtected` | `Boolean` | Somente atualização agendada (REQ-1141) |
| `offPeakWindow` | `String` | Janela de manutenção `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuso horário IANA |
| `cdc` | `SourceCdcConfigInput` | Configuração de transporte CDC do Kafka (REQ-824) |

### `TableInput`

[tool-verified: `provisa/api/admin/types.py:745-800`]

O input principal de registro de tabela. Campos-chave além do básico:

| Campo | Notas |
|-------|-------|
| `materialize` | Pousa uma cópia no armazenamento de materialização |
| `mvRefreshInterval` | Segundos entre atualizações |
| `mvPersist` | `replace` \| `append` \| `upsert` (REQ-965) |
| `mvIncremental` | Manutenção incremental (REQ-969) |
| `mvBitemporalMode` | `snapshot` \| `delta` para tabelas bitemporais (REQ-1162) |
| `mvCalendar` | Nome do calendário de snapshot (REQ-962) |
| `mvGrain` | Granularidade do snapshot: `daily`, `weekly`, `monthly`, `annual`, ou personalizada `3WE` / `LFR` (REQ-962) |
| `viewSql` | SQL para uma view derivada |
| `viewMetrics` | Especificação declarativa de view composta por métrica — mutuamente exclusiva com `viewSql` (REQ-1318) |
| `dqContract` | Texto de contrato de qualidade de dados em YAML/JSON (REQ-1443) |
| `queryTemplate` | Cypher para uma tabela Neo4j (REQ-1670) |
| `live` | Configuração de entrega ao vivo para push SSE/Kafka (REQ-565, REQ-813) |
| `discover` | Infere colunas a partir da fonte ao vivo no momento do registro (REQ-252) |

### `RelationshipInput`

[tool-verified: `provisa/api/admin/types.py:804-826`]

| Campo | Notas |
|-------|-------|
| `id` | Identificador do relacionamento |
| `sourceTableId` | Nome da tabela virtual (alias se definido, senão o nome da tabela) |
| `targetTableId` | Nome da tabela virtual; vazio para relacionamentos computados |
| `sourceColumn` | Coluna de junção no lado de origem |
| `targetColumn` | Coluna de junção no lado de destino |
| `cardinality` | `one-to-one` \| `one-to-many` \| `many-to-one` \| `many-to-many` |
| `alias` | Rótulo de aresta Cypher (ex.: `WORKS_FOR`) |
| `graphqlAlias` | Nome do campo GraphQL no tipo de origem |
| `viaTable` | Nome da tabela de junção para arestas muitos-para-muitos (REQ-1586) |
| `recordCandidate` | Também grava uma linha `accepted` em relationship_candidates |

---

## Exemplo: registrando uma tabela

```graphql
mutation {
  registerTable(input: {
    sourceId: "sales-pg"
    domainId: "sales"
    schemaName: "public"
    tableName: "orders"
    alias: "orders"
    columns: [
      { name: "id", visibleTo: ["public"], isPrimaryKey: true }
      { name: "customer_id", visibleTo: ["public"] }
      { name: "total", visibleTo: ["public"] }
    ]
  }) {
    success
    message
    code
  }
}
```

## Exemplo: criando uma regra de RLS

```graphql
mutation {
  upsertRlsRule(input: {
    tableId: "orders"
    roleId: "regional-analyst"
    filterExpr: "region = '{{user.region}}'"
  }) {
    success
    message
  }
}
```
