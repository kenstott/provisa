# Referência da API GraphQL de administração {: #admin-graphql-api-reference }

A API GraphQL de administração é o plano de configuração do Provisa. É a API que o aplicativo web de administração chama para toda operação de gerenciamento — criar fontes, registrar tabelas, definir relacionamentos, configurar regras de RLS, e tudo mais que molda o modelo.

**Ponto de montagem:** `POST /admin/graphql`

Esta não é a mesma API do plano de dados em `/data/graphql`. O plano de dados atende consultas de usuários finais sobre domínios registrados e é descrito pelo SDL em `/data/sdl`. A API de administração configura a aparência desse esquema e quem pode ver o quê.

---

## Como a interface conversa com esta API {: #how-the-ui-talks-to-this-api }

O aplicativo web de administração usa o Apollo Client, apontado para `${API_BASE}/admin/graphql`.

Toda requisição carrega um token bearer (buscado novamente do provedor de autenticação a cada chamada), um cabeçalho `X-Org-Id` quando multilocatário, e um cabeçalho `X-Env` ao atender um ambiente de branch.

O esquema é montado a partir de duas classes `@strawberry.type` — `Query` de `schema_query.py` e `Mutation` de `schema_mutation.py` — e envolvido em uma `ModelCommitExtension` que registra toda mutação contra o branch de ambiente atual.

---

## Autorização {: #authorization }

**Modo de desenvolvimento:** quando nenhuma autenticação está configurada e toda requisição chega como um principal anônimo, todas as verificações de capacidade são ignoradas. Isso mantém uma instalação local funcional sem configuração de autenticação.

**Portões de capacidade:** implantações de produção exigem capacidades nomeadas. O direito específico exigido por cada campo é anotado inline. Chamar uma mutação sem a capacidade exigida gera um `PermissionError`. A função de administrador da plataforma ignora todas as verificações de capacidade.

**Portões de domínio:** várias mutações também verificam o domínio ao qual o objeto pertence. Um chamador com escopo em `sales` não pode registrar uma tabela em `finance`, enfileirar uma regra de RLS para ela, ou criar um relacionamento cuja tabela de origem esteja em um domínio que ele não possui. Views são ainda mais restritas: toda tabela que o SQL da view lê deve estar dentro dos domínios do chamador, porque SQL livre de outra forma daria a um membro acesso a dados fora de seu escopo.

**Herança de função:** as capacidades de uma função pai são herdadas pelas funções filhas. `createRole` e `deleteRole` recusam ciclos e impedem excluir uma função que tenha herdeiras.

---

## Tipo de retorno comum {: #common-return-type }

A maioria das mutações retorna `MutationResult`.

```graphql
type MutationResult {
  success: Boolean!
  message: String!
  code: String          # stable i18n key, e.g. "schema.source_created"
  params: JSON          # key/value pairs for client-side localization
}
```

Quando uma mutação falha, `success` é `false` e `message` carrega o motivo em inglês. `code` é um identificador estável que a interface usa para renderizar uma mensagem localizada.

---

## Consultas {: #queries }

### Fontes {: #sources }

#### `sources → [SourceType!]!`

Todas as fontes de dados registradas.

```graphql
query {
  sources {
    id type host port database username dialect
    cacheEnabled cacheTtl replicate
    loadProtected offPeakWindow offPeakTz
    gqlNamingConvention path allowedDomains
    description mappingJson federationHintsJson
    changeSignal passwordRef
    cdc {
      bootstrapServers topicPrefix consumerGroupId
      schemaRegistryUrl schemaRegistryAuth schemaRegistryUsername schemaRegistryPassword
      schemaRegistryToken schemaRegistryClientCert schemaRegistryClientKey schemaRegistryCa
    }
  }
}
```

`passwordRef` é uma referência `${secret:NAME}` no cofre da organização — nunca a credencial literal.

#### `source(id: String!) → SourceType`

Uma única fonte por ID. Retorna `null` quando não encontrada.

#### `availableSchemas(sourceId: String!) → [String!]!`

Esquemas visíveis em uma fonte, filtrados para excluir os internos do Provisa. Usa introspecção nativa primeiro; recorre ao catálogo do motor quando o tipo de fonte não tem um pool direto.

#### `availableTables(sourceId: String!, schemaName: String = "public") → [AvailableTableType!]!`

Tabelas em um esquema de uma fonte, com seus comentários. Para fontes OpenAPI, retorna operações GET cuja resposta é um array ou wrapper de paginação. Para fontes GraphQL, retorna campos de consulta que retornam uma lista. Para gRPC, retorna RPCs de streaming do servidor.

#### `availableColumns(sourceId: String!, schemaName: String!, tableName: String!) → [String!]!`

Nomes de coluna para uma tabela no catálogo do motor. Para fontes govdata, usa um resolvedor separado.

#### `availableColumnsMetadata(sourceId: String!, schemaName: String!, tableName: String!) → [AvailableColumnType!]!`

Nomes de coluna com tipos de dados, comentários, tipos de filtro nativos, e sinalizadores de chave primária. Para fontes OpenAPI, deriva o formato a partir do esquema de resposta e dos parâmetros da operação.

#### `availableFunctions(sourceId: String!, schemaName: String = "openapi") → [AvailableTableType!]!`

Operações não-GET de uma fonte OpenAPI (POST, PUT, PATCH, DELETE). Retorna uma lista vazia para fontes que não são OpenAPI.

#### `crawlSource(path, depth, pattern, recursive, simpleLinks, sameDomain, excludePattern) → CrawlResultType`

Visualize o que um crawl de conector de arquivos descobriria — arquivos, tabelas e colunas — antes de uma fonte ser criada. As configurações exclusivas de HTTP (`simpleLinks`, `sameDomain`, `excludePattern`) são ignoradas para raízes local, S3, FTP e SFTP.

#### `suggestTableAlias(tableName: String!, domainId: String!, sourceId: String!) → String!`

Retorna o alias a usar ao registrar `tableName` em `domainId` a partir de `sourceId`. Retorna um alias simples em snake-case quando não há conflito, ou um alias prefixado pela fonte (`sqlite_b_orders`) quando o nome efetivo já está tomado por uma fonte diferente no mesmo domínio.

---

### Tabelas {: #tables }

#### `tables → [RegisteredTableType!]!`

Todas as tabelas registradas, cada uma com sua lista completa de colunas. A visibilidade de colunas na resposta respeita a capacidade `table_registration` do chamador — `canDeployToDb` é condicionado a ele possuir esse direito.

Cada `RegisteredTableType` expõe subcampos computados:

- **`refreshPolicySummary → RefreshPolicySummaryType`** — a política efetiva de atualização/serviço como texto simples, derivada no servidor a partir da mesma resolução do planejador que o motor usa. Retorna `null` durante a inicialização.
- **`graphqlFieldName → String`** — o nome do campo que esta tabela tem no esquema compilado do plano de dados, de modo que o painel de Produto de Dados possa montar um exemplo executável sem replicar o algoritmo de nomenclatura.
- **`dqDataset → String`** — esta tabela como um conjunto de dados de contrato de qualidade de dados, na forma que o verificador varre.
- **`productId → String`** — o produto de dados ao qual esta tabela pertence. Uma tabela verificadora de DQ herda o produto da tabela cujo contrato ela varre.

#### `refreshPolicyPreview(...) → RefreshPolicySummaryType`

Visualize o resumo efetivo de atualização/serviço para controles de tabela em *rascunho* (não salvos), de modo que o resumo no topo do formulário se atualize conforme os campos mudam sem persistir nada. Mesma derivação de `refreshPolicySummary` acima.

Argumentos: `sourceId`, `domainId`, `schemaName`, `tableName`, `cacheTtl`, `replicate`, `loadProtected`, `offPeakWindow`, `offPeakTz`, `changeSignal`.

#### `columnDependents(tableId: String!, renamed: [String!], removed: [String!]) → [ColumnDependentsType!]!`

Artefatos que uma renomeação de alias ou exclusão de coluna pendente quebraria. Consultivo — a interface de administração mostra isso antes de salvar e o administrador decide. Deve ser chamado *antes* de salvar, porque os dependentes foram escritos contra o nome exposto que a coluna carrega atualmente.

---

### Relacionamentos {: #relationships }

#### `relationships → [RelationshipType!]!`

Todos os relacionamentos definidos pelo usuário (exclui entradas `gql_auto__` geradas automaticamente e as entradas sintéticas `meta:%` usadas pelo ERD).

#### `allRelationships → [RelationshipType!]!`

Igual a `relationships`, mas inclui as entradas sintéticas `meta:%`. Usado pelo ERD em grafo, que precisa mostrar toda aresta, incluindo os links implícitos `HAS_TABLE` entre tabelas de dados e o registro de metadados.

Cada `RelationshipType` expõe:

- **`autoSuggested → Boolean`** — se o relacionamento foi sugerido pela análise de FK (o `id` começa com `fk__`).
- **`physicalName → String`** — o nome do relacionamento nos planos SQL e gRPC (o parâmetro `?include=`). Derivado no servidor; clientes não devem transliterar o alias GraphQL.

---

### Domínios, funções e usuários {: #domains-roles-users }

#### `domains → [DomainType!]!`

Todos os domínios no banco de dados do locatário da organização ativa. O banco de dados do locatário é isolado em nível de esquema, portanto a lista de domínios de um administrador de organização contém apenas as linhas de sua própria organização.

#### `roles → [RoleType!]!`

Funções visíveis ao chamador. Um administrador vê toda função; um não administrador vê apenas funções sem `org_id` ou funções pertencentes à sua organização.

#### `resolveOwners(refs: [String!]!) → [UserSummaryType!]!`

Resolve IDs de função ou IDs de usuário para usuários individuais. Usado para expandir `DataProduct.ownerRole`, `Domain.steward` e `Column.visibleTo` em uma lista legível por humanos. Referências desconhecidas são ecoadas de volta puras, de modo que a interface mostre o ID bruto em vez de nada.

---

### Regras RLS {: #rls-rules }

#### `rlsRules → [RLSRuleType!]!`

Todas as regras de segurança em nível de linha. O repositório subjacente descriptografa `filterExpr` na borda.

---

### Produtos de dados {: #data-products }

#### `dataProducts → [DataProductType!]!`

Todos os produtos de dados. Exige a capacidade `data_product_read`.

---

### Tags

#### `tags → [TagType!]!`

Todas as definições de tag, incluindo seus valores de parâmetro permitidos por tag.

#### `tagAssignments → [TagAssignmentType!]!`

Todas as atribuições de tag entre fontes, tabelas, colunas e relacionamentos.

---

### Views materializadas {: #materialized-views }

#### `mvList → [MVType!]!`

Todas as views materializadas com seu status em tempo de execução: habilitada/desabilitada, timestamp da última atualização, contagem de linhas, e último erro.

---

### Cache

#### `cacheStats → CacheStatsType`

Estatísticas de cache. Retorna `storeType: "redis"` com métricas operacionais completas quando o Redis está configurado, `storeType: "memory"` para o armazenamento fakeredis embutido, e `storeType: "noop"` quando nenhum cache está configurado.

#### `cacheTableStats → [CacheTableStatType!]!`

Contagens de entradas em cache por tabela. Vazio quando nenhum armazenamento de cache está configurado.

#### `hotTables → [HotTableStatType!]!`

Tabelas das quais o Provisa mantém uma cópia, em duas camadas: `hot` (espelhada no armazenamento de resposta para inlining de JOIN) e `warm` (pousada como uma cópia Iceberg). Uma tabela está em no máximo uma camada.

#### `materializeStoreInfo → MaterializeStoreInfoType`

Identidade do armazenamento de materialização durável: nome do motor, referência de DSN do armazenamento, contagem de MVs, e se o armazenamento é local à instância (um arquivo local como DuckDB ou SQLite, o que significa que cada instância atrás de um balanceador de carga mantém sua própria cópia).

---

### Saúde do sistema {: #system-health }

#### `systemHealth → SystemHealthType`

Status de conexão do motor, contagens de pool de workers, estado do pool do banco de dados de metadados, modo de cache, e vivacidade de todo listener de protocolo (pgwire, gRPC, Arrow Flight, Bolt).

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

### Tarefas agendadas {: #scheduled-tasks }

#### `scheduledTasks → [ScheduledTaskType!]!`

Gatilhos agendados a partir da configuração, com estado em tempo de execução. Cada entrada carrega sua expressão cron, `kind` (`webhook` ou `sql`), se está habilitada atualmente, o timestamp da última execução (sempre `null` nesta versão — rastreado pelo agendador), e o próximo horário de execução agendado a partir do APScheduler.

---

### Qualidade de dados {: #data-quality }

#### `dqContractParse(checker: String!, contractText: String!) → DqContractType`

Analisa texto de contrato bruto nas linhas editáveis do painel construtor. Chamado a cada edição; uma falha de análise volta como `error` em vez de um erro GraphQL, porque texto parcialmente escrito é normal enquanto o operador está digitando.

#### `dqCheckCatalog(checker: String!, dataset: String!) → DqCheckCatalogType`

As verificações que `checker` oferece, restritas às colunas de `dataset`. O conjunto de dados é o alvo observado do contrato, resolvido da mesma forma que o scanner o resolve — de modo que as verificações oferecidas correspondam às colunas que o verificador realmente verá.

#### `dqCheckDefinition(checker: String!, check: DqCheckBuildInput!) → DqCheckDefinitionType`

O texto de uma verificação a partir dos editores do painel. No servidor porque o dialeto tem uma única implementação; uma verificação feita pelo construtor e uma digitada manualmente devem ser indistinguíveis.

#### `dqContractBuild(checker: String!, dataset: String!, checks: [DqCheckInput!]!) → DqContractTextType`

Serializa linhas de verificação editadas de volta em texto de contrato. O inverso de `dqContractParse`. No servidor pelo mesmo motivo: o painel não pode emitir texto que o verificador recusaria.

---

### Pré-visualizações de fonte {: #source-previews }

#### `neo4jPreview(sourceId: String!, cypher: String!) → QueryPreviewType`

Pré-visualize uma projeção Cypher em uma fonte Neo4j: até cinco linhas e os tipos de coluna que o registro carregará. Falhas voltam como `error`.

#### `sparqlPreview(sourceId: String!, query: String!) → QueryPreviewType`

Pré-visualize um SELECT SPARQL em uma fonte SPARQL: até cinco linhas, todas as colunas como texto.

---

### Kaggle

#### `kaggleTokenValid(token: String!) → Boolean!`

Verificação em tempo real contra a API Kaggle. Retorna `true` apenas quando o token se autentica. Sustenta a etapa de portão de token no formulário de fonte Kaggle.

#### `kaggleDatasets(token: String!, query: String = "", page: Int = 1) → [KaggleDatasetType!]!`

Busca no catálogo completo de conjuntos de dados públicos do Kaggle. Restrito por token.

---

### Calendários {: #calendars }

#### `calendars → [CalendarType!]!`

Todas as versões registradas de calendário de limite de snapshot. Alimenta o seletor de configuração de agenda de snapshot e confirma quais calendários uma MV periódica pode referenciar.

---

### Métricas {: #metrics }

#### `metrics → [MetricType!]!`

Todas as definições de métrica governadas. Métricas derivadas de fato carregam `fromFact`.

---

### Versão do esquema {: #schema-version }

#### `schemaVersion → String!`

Hash SHA-256 do estado atual do esquema (domínios, IDs de tabela, IDs de relacionamento). O cliente Apollo lê isso do cabeçalho de resposta `X-Schema-Version` e refaz todas as consultas ativas quando ele avança.

---

### Auxiliares de IA {: #ai-helpers }

#### `generateTableDescription(tableId: String!) → String!`

Usa o LLM configurado para gerar uma descrição de uma a duas frases para uma tabela registrada. Salve a tabela primeiro; chamar isso em uma tabela não salva retorna uma mensagem instrucional.

#### `generateColumnDescription(tableId: String!, columnName: String!) → String!`

Usa o LLM configurado para gerar uma descrição de uma frase para uma única coluna.

---

### Requisições de criação {: #creation-requests }

#### `creationRequests → [CreationRequestType!]!`

Solicitações de criação pendentes, visíveis para chamadores com a capacidade de criação correspondente. Usado quando um membro sem `create_relationship` ou `create_view` envia uma solicitação que um detentor do direito precisa aprovar. Uma solicitação de relacionamento é listada para os usuários cujo direito `create_relationship` alcança o domínio da sua tabela de origem ou de destino, e para o usuário que a fez.

---

## Mutações {: #mutations }

### Fontes {: #sources_1 }

#### `createSource(input: SourceInput!) → MutationResult`

Registra uma nova fonte de dados. Um id que já existe é recusado (`schema.source_id_taken`) depois das verificações da definição e antes que qualquer coisa seja preparada ou gravada; uma fonte é alterada com `updateSource`. Valida a conexão antes de persistir — uma fonte recusada não deixa nenhuma entrada no cofre. Armazena as credenciais no cofre da organização e registra a referência; o texto em claro nunca chega ao banco de dados. Requer a capacidade `source_registration`.

#### `updateSource(input: SourceInput!) → MutationResult`

Atualiza os detalhes de conexão, a descrição e a configuração de uma fonte existente, depois a provisiona de novo no motor vinculado, reconstrói os esquemas e reconcilia suas tabelas pousadas, para que uma fonte salva sob um motor seja servida pelo motor para o qual se mudou depois. Desmonta e reconecta o endpoint pgwire das fontes de arquivos/SharePoint para que uma mudança de caminho tenha efeito imediato. Requer `source_registration`.

#### `deleteSource(id: String!) → MutationResult`

Remove uma fonte e sua entrada no cofre. Descarta o catálogo do motor e reconstrói os esquemas.

#### `renameSource(oldId: String!, newId: String!) → MutationResult`

Renomeia o id de uma fonte. Um novo id que outra fonte possui é recusado (`schema.source_id_taken`) e nada é gravado; renomear uma fonte para o seu próprio id não muda nada.

#### `updateSourceCache(sourceId: String!, cacheEnabled: Boolean!, cacheTtl: Int) → MutationResult`

Habilita ou desabilita o cache de resultado de consulta para uma fonte, e define o TTL em segundos.

#### `updateSourceReplicate(sourceId: String!, replicate: Int) → MutationResult`

Define quando as tabelas de uma fonte são servidas das suas réplicas: `null` Default, `-1` Never, `N` acima de 0 assim que uma tabela passa de N instruções governadas por intervalo, `0` Always. Uma tabela sem valor próprio o herda. Recusado quando combinaria `load_protected` com `-1`, ou deixaria sem `cache_ttl` uma tabela `ttl` que replica. Requer `source_registration`.

#### `updateSourceLoadProtection(sourceId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Marca uma fonte como protegida contra carga (somente atualização agendada). Exige pelo menos um portão — janela fora de pico, cadência de TTL de cache, ou um sinal de mudança por sondagem — ou a chamada falha.

#### `updateSourceNaming(sourceId: String!, gqlNamingConvention: String) → MutationResult`

Define a convenção de nomenclatura GraphQL por fonte.

#### `updateSourceAllowedDomains(sourceId: String!, allowedDomains: [String!]!) → MutationResult`

Define quais domínios podem usar uma fonte (lista vazia = irrestrito). Exige `source_registration`.

#### `stageKaggleDataset(token, owner, ref, idPrefix) → KaggleStageResultType`

Baixa e descompacta um conjunto de dados Kaggle no disco local. Retorna o caminho do diretório preparado; o chamador então cria uma fonte do tipo `files` apontando para ele. Pacotes contendo SQLite são rejeitados por inteiro. Exige `source_registration`.

#### `refreshKaggleSource(sourceId: String!, token: String!) → MutationResult`

Rebusca o conjunto de dados de uma fonte derivada do Kaggle no local. Pula o download se o Kaggle não tiver nada mais recente do que o que está em disco. Exige `source_registration`.

#### `refreshSourceStatistics(sourceId: String!) → MutationResult`

Executa `ANALYZE` em todas as tabelas registradas de uma fonte. Melhora as decisões de ordem de junção e broadcast para consultas federadas.

---

### Tabelas {: #tables_1 }

#### `registerTable(input: TableInput!) → MutationResult`

Registra uma nova tabela (ou view) em um domínio. Exige a capacidade `table_registration` e associação ao domínio alvo. Um chamador sem `create_relationship` que envia uma view é enfileirado como uma requisição de criação para um detentor de direitos aprovar.

#### `updateTable(input: TableInput!) → MutationResult`

Atualiza o alias, descrição, metadados de coluna, configurações de MV e configuração de entrega ao vivo de uma tabela existente. Exige `table_registration`.

#### `deleteTable(id: Int!) → MutationResult`

Exclui uma tabela registrada. Consulta o domínio da tabela para o portão de domínio antes de excluir.

#### `updateTableCache(tableId: Int!, cacheTtl: Int) → MutationResult`

Sobrepõe o TTL de cache para uma tabela.

#### `updateTableReplicate(tableId: Int!, replicate: Int) → MutationResult`

Define quando uma tabela é servida da sua réplica. `null` = herdar o valor da fonte. Mesmos valores e recusas de `updateSourceReplicate`. Requer `table_registration`.

#### `updateTableLoadProtection(tableId, loadProtected, offPeakWindow, offPeakTz) → MutationResult`

Sobrepõe a proteção de carga para uma tabela. `null` para `loadProtected` herda o padrão da fonte. Valida a combinação efetiva de portão (tabela → fonte).

#### `updateTableNaming(tableId: Int!, gqlNamingConvention: String) → MutationResult`

Define a convenção de nomenclatura GraphQL por tabela.

#### `deployViewToDb(tableId: Int!) → MutationResult`

Promove uma view virtual do Provisa para uma view real de banco de dados em sua fonte nativa subjacente.

#### `forceRegen(tableId: Int!, reason: String!) → MutationResult`

Recalcula as linhas pousadas de uma tabela sob demanda, contornando o portão de mudança normal. `reason` é uma anotação de auditoria obrigatória. Recusado para tabelas federadas ao vivo (sem linhas pousadas).

#### `invalidateFileSource(tableId: Int!) → MutationResult`

Força o próximo acesso de uma tabela de conector de arquivo SQLite a resincronizar a partir do disco.

#### `registerEntity(input: EntityInput!) → MutationResult`

Açúcar sintático para registrar uma entidade de dimensão/hub. Rebaixa para uma MV (bitemporal, quando historizada) e chama `registerTable`.

#### `registerFact(input: FactInput!) → MutationResult`

Açúcar sintático para registrar um fato de esquema estrela. Rebaixa para uma MV agregada, cria relacionamentos de dimensão, e registra automaticamente as medidas de fato como métricas governadas.

---

### Relacionamentos {: #relationships_1 }

#### `createRelationship(input: RelationshipInput!) → MutationResult`

Cria um relacionamento. A verificação confere o domínio da tabela de origem (não o do destino). Uma aresta entre domínios é armazenada com `needsReview: true`. Um chamador sem `create_relationship` é enfileirado como solicitação de criação. Arestas de junção (muitos-para-muitos) exigem listas de chaves de mesmo comprimento. Recusado pelo nome, antes de gravar qualquer coisa: um id que já existe (`schema.relationship_id_taken`); um nome de tabela que não resolve para nenhuma tabela registrada ou para mais de uma (`schema.relationship_unknown_source`, `_target`, `_junction`, `schema.relationship_ambiguous_table`); um alias Cypher ou alias GraphQL que outro relacionamento da mesma tabela de origem já tem (`schema.relationship_alias_taken`, `schema.relationship_graphql_alias_taken`). Uma tabela é nomeada pelo seu nome virtual: o seu alias quando tem um.

#### `updateRelationship(input: RelationshipInput!) → MutationResult`

Altera um relacionamento existente, com as mesmas verificações de `createRelationship`. Um id que nenhum relacionamento tem é recusado (`schema.relationship_not_found`). Uma alteração exige o próprio direito; nunca é enfileirada como solicitação.

#### `deleteRelationship(id: String!) → MutationResult`

Exclui um relacionamento por ID e reconstrói os esquemas.

---

### Domínios {: #domains }

#### `createDomain(input: DomainInput!) → MutationResult`

Cria um domínio. Um id que já existe é recusado (`schema.domain_id_taken`). Palavras de segmento reservadas (`tables`, `relationships` e outros segmentos de caminho de URI) são recusadas, assim como o literal curinga `*`. Requer a capacidade `org_settings`.

#### `deleteDomain(id: String!) → MutationResult`

Exclui um domínio. Exige `org_settings`.

#### `updateGqlNamingConvention(convention: String!) → MutationResult`

Define a convenção global de nomenclatura GraphQL e reconstrói os esquemas para todas as funções. Apenas nomes de convenção reconhecidos são aceitos.

---

### Funções {: #roles }

#### `createRole(input: RoleInput!) → MutationResult`

Cria uma função com capacidades, acesso a domínios, limites de taxa opcionais e uma função pai opcional. Valida que o pai existe e que a cadeia de pais não tem ciclos. Depois das verificações da definição, um id que já existe é recusado (`schema.role_id_taken`). Requer `user_management`.

#### `updateRole(input: RoleInput!) → MutationResult`

Altera uma função existente, com as mesmas verificações de `createRole`. Um id que nenhuma função tem é recusado (`schema.role_not_found`). Redefinir uma função é um ato em cada domínio que ela alcança.

#### `deleteRole(id: String!) → MutationResult`

Exclui uma função. Falha se outras funções herdam dela — reatribua o pai delas primeiro. Exige `user_management`.

---

### Regras RLS {: #rls-rules_1 }

#### `upsertRlsRule(input: RLSRuleInput!) → MutationResult`

Cria ou atualiza uma regra de segurança em nível de linha. A expressão de filtro é validada no momento de salvar contra as colunas da tabela ou domínio alvo, de modo que uma regra que o administrador não consegue consultar seja recusada com o motivo em vez de falhar silenciosamente no momento da consulta. Exige `masking_config`.

Os alvos são mutuamente exclusivos: defina `tableId` para uma regra em nível de tabela, `domainId` para uma regra em nível de domínio, ou `actionName` para uma função/webhook rastreado.

#### `deleteRlsRule(roleId, tableId, domainId, actionName) → MutationResult`

Exclui uma regra de RLS. Exige `masking_config`.

---

### Produtos de dados {: #data-products_1 }

#### `createDataProduct(input: DataProductInput!) → MutationResult`

Cria um produto de dados. Um id que já existe é recusado (`schema.data_product_id_taken`). Requer a capacidade `data_product_rw`.

#### `updateDataProduct(input: DataProductInput!) → MutationResult`

Altera um produto de dados existente. Um id que nenhum produto tem é recusado (`schema.data_product_not_found`). Requer `data_product_rw`.

#### `deleteDataProduct(id: String!) → MutationResult`

Exclui um produto de dados. Exige `data_product_rw`.

---

### Tags

#### `createTag(input: TagInput!) → MutationResult`

Cria uma definição de tag. Um id que já existe é recusado (`schema.tag_id_taken`). Tags de sistema e tags derivadas não podem ser redefinidas. `appliesTo` deve ser um subconjunto não vazio de `["source", "table", "column", "relationship", "command"]`.

#### `updateTag(input: TagInput!) → MutationResult`

Altera uma definição de tag existente, com as mesmas verificações de `createTag`. Um id que nenhuma tag tem é recusado (`schema.tag_not_found`).

#### `deleteTag(id: String!) → MutationResult`

Exclui uma tag. Recusa tags de sistema e derivadas.

#### `assignTag(input: TagAssignmentInput!) → MutationResult`

Atribui uma tag a uma fonte, tabela, coluna, relacionamento, ou command. Aplica as políticas de campo da tag (`reason_policy`, `expires_policy`) e — para tags parametrizadas — valida o valor do parâmetro contra a lista permitida da tag.

#### `unassignTag(input: TagAssignmentInput!) → MutationResult`

Remove uma atribuição de tag.

#### `upsertTagParamValue(input: TagParamValueInput!) → MutationResult`

Adiciona ou redescreve um valor de parâmetro permitido para uma tag parametrizada. A lista de valores permitidos é fechada: toda atribuição deve nomear um valor dela.

#### `deleteTagParamValue(tagId: String!, value: String!) → MutationResult`

Remove um valor permitido. Recusado enquanto qualquer atribuição ainda o carregar, porque essas atribuições nomeariam um tipo que a lista não admite mais.

---

### Métricas {: #metrics_1 }

#### `createMetric(input: MetricInput!) → MutationResult`

Cria uma definição de métrica governada. Um nome que já existe é recusado (`schema.metric_name_taken`). A expressão deve ser analisável pelo sqlglot e conter pelo menos uma função de agregação. Regenera todas as views compostas por métricas que referenciam esta métrica. Requer `table_registration`.

#### `updateMetric(input: MetricInput!) → MutationResult`

Substitui a definição de uma métrica existente, com as mesmas verificações de `createMetric`. Um nome que nenhuma métrica tem é recusado (`schema.metric_not_found`). Salvá-la exige os domínios da nova expressão e da definição que ela substitui.

#### `deleteMetric(name: String!) → MutationResult`

Exclui uma métrica governada. Reconstrói os esquemas. Exige `table_registration`.

---

### Calendários {: #calendars_1 }

#### `createCalendar(input: CalendarInput!) → MutationResult`

Cria ou substitui um calendário versionado de limite de snapshot. Validado construindo o `Calendar` em memória antes de persistir — falha em um sistema base desconhecido, fuso horário inválido, ou âncora fiscal inválida.

#### `deleteCalendar(name: String!) → MutationResult`

Exclui um calendário (todas as versões). Recusado quando qualquer view materializada o referencia.

---

### Views materializadas {: #materialized-views_1 }

#### `refreshMv(mvId: String!) → MutationResult`

Dispara uma atualização manual de uma view materializada. Coordena entre a frota quando o modo de consistência da MV é `shared`.

#### `toggleMv(mvId: String!, enabled: Boolean!) → MutationResult`

Habilita ou desabilita uma view materializada.

---

### Cache

#### `purgeCache → MutationResult`

Purga todos os resultados de consulta em cache.

#### `purgeCacheByTable(tableId: Int!) → MutationResult`

Purga os resultados em cache de uma tabela.

---

### Tarefas agendadas {: #scheduled-tasks_1 }

#### `createScheduledTask(id, name, cron, kind, webhookName, argsJson, sql) → MutationResult`

Cria um gatilho agendado — uma chamada de webhook ou uma instrução SQL — e o registra ao vivo no APScheduler. `kind` é `"webhook"` ou `"sql"`.

#### `deleteScheduledTask(taskId: String!) → MutationResult`

Remove um gatilho agendado da configuração e do agendador ao vivo.

#### `toggleScheduledTask(taskId: String!, enabled: Boolean!) → MutationResult`

Habilita ou desabilita uma tarefa agendada no arquivo de configuração.

---

### Qualidade de dados {: #data-quality_1 }

#### `dryRunDqContract(sourceId: String!, contractText: String!) → DqDryRunType`

Executa um contrato contra a tabela ao vivo e retorna os resultados sem pousar nada. Uma mutação em vez de uma query porque custa uma varredura real. O que ela prova é se o identificador do conjunto de dados resolve para a tabela governada que o operador pretende.

#### `runDqCheckNow(schemaName: String!, tableName: String!) → MutationResult`

Dispara imediatamente o job de sondagem de uma tabela verificadora. Pousa linhas da forma normal, de modo que os resultados persistam e o histórico de DQ mostre a nova varredura.

---

### Manutenção de esquema {: #schema-maintenance }

#### `rebuildSchemas → MutationResult`

Reconstrói o esquema em memória a partir do estado do banco de dados. Útil após mudanças externas no banco de dados.

---

### Requisições de criação {: #creation-requests_1 }

#### `approveCreationRequest(requestId: Int!) → MutationResult`

Aprova uma solicitação de criação: a mesma regra, as mesmas recusas e as mesmas entradas do registro de administração de `POST /admin/creation-requests/{id}/approve`. O solicitante não pode aprovar a própria solicitação e a aprovação de um usuário conta uma vez. A aprovação que completa a contagem da solicitação cria o que ela pede; `params.status` é `executed` quando foi esta, senão `pending`, e `params.waitingOn` lista os domínios que uma solicitação de relacionamento ainda aguarda.

#### `executeCreationRequest(requestId: Int!) → MutationResult`

Executa uma solicitação de criação que já teve suas aprovações — relacionamento, view, tabela, fonte ou webhook — pelo código que a mutação direta usa. Recusada com `requests.approvals_incomplete` até que as aprovações exigidas da solicitação (uma, ou duas para um relacionamento) sejam atingidas por usuários diferentes do solicitante; uma solicitação de relacionamento também é recusada com `requests.waiting_on_domains` enquanto um domínio que ela toca não tiver aprovador. As aprovações são dadas por `POST /admin/creation-requests/{id}/approve`, e a aprovação que completa a contagem faz a criação, então esta mutação serve para tentar de novo uma criação que falhou. Uma criação que falha deixa a solicitação pendente e responde com o código próprio da falha.

#### `rejectCreationRequest(requestId: Int!, reason: String!) → MutationResult`

Rejeita uma solicitação enfileirada com um motivo acionável. `reason` é obrigatório. Uma solicitação de relacionamento pode ser rejeitada por qualquer usuário que poderia aprová-la. O autor de uma solicitação de qualquer outro tipo pode retirá-la; isso fica registrado no registro de administração como uma retirada.

---

### Compilação de consulta {: #query-compilation }

#### `compileQuery(input: CompileQueryInput!) → [CompileQueryResult!]!`

Compila uma consulta GraphQL do plano de dados contra o esquema de uma função e retorna a decisão completa de roteamento: SQL semântico, SQL do motor, SQL direto, rota, metadados de aplicação (filtros de RLS aplicados, colunas excluídas, mascaramento aplicado), e Cypher compilado. Retorna um resultado por campo raiz na consulta.

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

## Tipos de entrada principais {: #key-input-types }

### `SourceInput`

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
| `changeSignal` | `String` | `ttl` \| `probe` \| `ttl_probe` |
| `loadProtected` | `Boolean` | Somente atualização agendada |
| `offPeakWindow` | `String` | Janela de manutenção `HH:MM-HH:MM` |
| `offPeakTz` | `String` | Fuso horário IANA |
| `cdc` | `SourceCdcConfigInput` | Configuração do transporte Kafka CDC, incluindo como seu schema registry é alcançado (veja [Autenticação do schema registry](subscriptions.md#schema-registry-authentication)). `schemaRegistryPassword` e `schemaRegistryToken` são retornados como referências do cofre, nunca como credenciais. |

### `TableInput`

O input principal de registro de tabela. Campos-chave além do básico:

| Campo | Notas |
|-------|-------|
| `materialize` | Pousa uma cópia no armazenamento de materialização |
| `mvRefreshInterval` | Segundos entre atualizações |
| `mvPersist` | `replace` \| `append` \| `upsert` |
| `mvIncremental` | Manutenção incremental |
| `mvBitemporalMode` | `snapshot` \| `delta` para tabelas bitemporais |
| `mvCalendar` | Nome do calendário de snapshot |
| `mvGrain` | Granularidade do snapshot: `daily`, `weekly`, `monthly`, `annual`, ou personalizada `3WE` / `LFR` |
| `viewSql` | SQL para uma view derivada |
| `viewMetrics` | Especificação declarativa de view composta por métrica — mutuamente exclusiva com `viewSql` |
| `dqContract` | Texto de contrato de qualidade de dados em YAML/JSON |
| `queryTemplate` | Cypher para uma tabela Neo4j |
| `live` | Configuração de entrega ao vivo para push SSE/Kafka |
| `discover` | Infere colunas a partir da fonte ao vivo no momento do registro |

### `RelationshipInput`

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
| `viaTable` | Nome da tabela de junção para arestas muitos-para-muitos |
| `recordCandidate` | Também grava uma linha `accepted` em relationship_candidates |

---

## Exemplo: registrando uma tabela {: #example-registering-a-table }

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

## Exemplo: criando uma regra de RLS {: #example-creating-an-rls-rule }

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
