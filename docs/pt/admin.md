# API de Administração

A API de administração é um endpoint Strawberry GraphQL em `POST /admin/graphql` (REQ-533). Ela exige uma função superuser ou admin (REQ-125, REQ-060) e é separada do endpoint GraphQL de dados (REQ-533).

## Autenticação

Passe suas credenciais no cabeçalho `Authorization` usando o provedor de autenticação padrão do Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

O acesso administrativo é governado pela capacidade `admin` atribuída a uma função (REQ-060, REQ-042).

### Tokens de acesso pessoal

Um token de acesso pessoal é aceito em qualquer lugar onde um token bearer seja aceito, incluindo este endpoint. A emissão e a revogação são de autoatendimento — é a credencial privada de quem a possui, portanto reside no perfil do usuário na UI de administração e não em uma página de administrador, ao lado de sair de uma organização e excluir a conta. Um administrador não emite tokens em nome de outra pessoa. (REQ-1263)

| Rota | Efeito |
| ------- | -------- |
| `POST /auth/tokens` | Emite um token para quem chama. Corpo: `name`, mais opcionalmente `role_id`, `scopes`, `expires_in_days` (1–366). A resposta é o único lugar onde o segredo aparece |
| `GET /auth/tokens` | Os tokens ativos de quem chama nesta organização — prefixo de exibição, nome, marcas temporais do ciclo de vida e o hash que identifica um token para revogação. Nunca uma credencial utilizável |
| `DELETE /auth/tokens/{token_hash}` | Revoga um dos tokens de quem chama. 404 quando não é seu ou já foi revogado |

Omitir `role_id` deixa o token resolver para a função que o proprietário possui; nomear uma função restringe o token abaixo do seu proprietário. A revogação também acontece implicitamente: remover a participação de um usuário em uma organização revoga seus tokens para aquela organização. Para a credencial em si, veja o [modelo de segurança](security.md#tokens-de-acesso-pessoal).

## Capacidades

### Gerenciamento de Config

Baixe a config atualmente em execução (REQ-164):

```http
GET /admin/config
```

Retorna o `config.yaml` completo como um arquivo YAML. Envie uma nova config (REQ-164):

```http
PUT /admin/config
```

O Provisa valida o YAML, recarrega catálogos, e regenera esquemas (REQ-012, REQ-253). Nenhum reinício necessário.

### Configurações de Runtime

Leia e escreva configurações de plataforma em tempo de execução sem editar o arquivo de config (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

A superfície de configurações cobre redirecionamento de resultado grande, amostragem padrão e limite de linha, TTL de cache de resposta, convenção de nomenclatura, auto-rastreamento de FK de relacionamento, DSN do armazenamento de materialização, memória do motor de federação (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), e a superfície completa de ajuste do pipeline de rastreamento OpenTelemetry (REQ-1082). Limites de travessia GraphQL remota e configurações de camada quente/cache de leitura também são expostos (REQ-1081, REQ-1083).

Postura de segurança — `security.mode` (`standard` | `high`) — aplicada no reinício (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Atribuições de modelo de IA, o registro de modelo de embedding/vetor, e o limite de taxa NL — aplicados no reinício (REQ-1080):

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

A aba de criptografia da administração deriva sua lista de provedores ao vivo a partir do registro de criptografia; provedores indisponíveis aparecem mas não são selecionáveis (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` são sempre não autenticados — eles contornam a exigência de `Authorization: Bearer` mesmo quando um provedor de autenticação está configurado (REQ-539).

### Editor de Relacionamento

Liste relacionamentos (REQ-166):

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Crie um relacionamento (REQ-019):

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### Descoberta de Relacionamento por IA

Dispare a análise de FK alimentada por Claude via REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Retorna candidatos a FK classificados por confiança. Aceite um candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspecção de Esquema

Navegue por tabelas publicadas em todas as fontes (REQ-008):

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Verificação de dependência de coluna (REQ-1484)

Antes de salvar uma edição de tabela que renomeia o alias SQL de uma coluna ou remove uma coluna,
pergunte o que mais referencia essa coluna:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renomear um alias quebra todo artefato definido contra o nome exposto — views, MVs, expressões de
métrica, predicados RLS, contratos DQ. Remover uma coluna quebra esses mais os artefatos que
armazenam o `column_name` físico: relacionamentos, vínculos de glossário, atribuições de tag.
`breaksOn` indica qual. A página Tables executa isso ao salvar e mostra o resultado como um diálogo
consultivo. Veja [Lineage](lineage.md) para o que a consulta cobre e o que ela não cobre.

### Gerenciamento de View

Registre uma view materializada (REQ-133, REQ-135):

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Dispare uma atualização manual (REQ-135):

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de Fonte de Grafo

Fontes Neo4j e SPARQL são registradas via endpoints REST (não a API de administração GraphQL) (REQ-295, REQ-297):

**Neo4j:**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL:**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Uma vez registradas, tabelas aparecem no esquema GraphQL e são consultáveis como qualquer outra fonte (REQ-016).

## GraphiQL

A API de administração vem com GraphiQL em `GET /admin/graphql` no navegador (REQ-622). Use-o para explorar o esquema de administração completo interativamente.

## Visões de gestão do domínio ops (REQ-1386)

Oito visões SQL são semeadas no domínio integrado `ops` em toda instalação. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Elas expõem o registro de auditoria de consultas como tabelas governadas — consultáveis via SQL (pgwire), GraphQL e Cypher, sob as mesmas regras de acesso ao domínio, RLS e mascaramento de qualquer tabela de negócio.

`org_admin` é definido como steward do domínio ops no momento da semeadura, de modo que o domínio nunca aparece como lacuna de governança em `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Visão | O que responde |
| --- | --- |
| `usage_ranking` | Contagem de consultas e usuários distintos por tabela registrada; tabelas sem acessos aparecem como candidatas à desativação |
| `deprecated_usage` | Todo acesso a uma tabela ou coluna com a tag `deprecated` — os consumidores ativos que impedem uma remoção segura |
| `pii_access` | Todo acesso a uma tabela ou coluna com a tag `pii`: quem consultou, sob qual função, através de qual superfície |
| `policy_denials` | Todas as tentativas de acesso que a governança recusou (HTTP 401/403) |
| `surface_mix` | Contagem diária de consultas e usuários distintos por superfície de protocolo (SQL, GraphQL, Cypher, gRPC etc.) |
| `query_health` | Contagem diária de erros e latência média/máxima por superfície |
| `stale_metadata` | Tabelas e colunas sem descrição; domínios sem steward |
| `join_hotspots` | Os pares de tabelas consultados juntos com mais frequência — candidatos a materialização ou cache |

Dois limites se aplicam hoje. A resolução é no nível da tabela — o registro de auditoria armazena `table_ids`, não as colunas individuais acessadas. O texto da consulta é cifrado (REQ-689) e excluído de todas as visões aqui; ele só é acessível pelo caminho administrativo autorizado de decifragem. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Uma função precisa de acesso ao domínio `ops` para que essas visões sejam visíveis. Conceda-o como você concederia acesso a qualquer outro domínio.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

As mesmas consultas funcionam como GraphQL ou Cypher sobre qualquer transporte governado — pgwire, Arrow Flight ou Bolt. [inferred from governed-surface design]

## Visualizador de relatórios (REQ-1390)

O visualizador de relatórios fica em `/admin/reports`. Funções sem a capacidade `observability` não conseguem acessá-lo.

O painel esquerdo lista todas as tabelas registradas no domínio `ops`, ordenadas por alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] As oito visões de gestão semeadas aparecem ali automaticamente. Clique em qualquer relatório para carregá-lo no visualizador de dados governado à direita.

**Adicionar um relatório personalizado.** O botão "Adicionar relatório" abre um diálogo. Forneça um nome, uma descrição opcional e uma instrução SELECT. Salvar registra a visão como tabela derivada governada no domínio `ops` — catalogada, com controle de acesso e consultável por todas as superfícies junto às visões semeadas. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Exclusão.** O ícone de lixeira aparece apenas para relatórios personalizados. Visões de gestão semeadas não podem ser excluídas por esta superfície. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Pré-visualização de tabela (REQ-1392)

Expanda qualquer linha na página de Tabelas. O botão **Pré-visualizar** abre um modal com 90% de largura contendo os dados governados ao vivo da tabela. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Tabelas apoiadas em APIs com parâmetros de caminho obrigatórios bloqueiam a pré-visualização até que esses valores sejam fornecidos. Um formulário embutido coleta cada parâmetro obrigatório antes da execução da primeira consulta; parâmetros de consulta opcionais aparecem no mesmo formulário. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visualizador de dados governado (REQ-1391)

O mesmo componente visualizador alimenta o modal de pré-visualização e o visualizador de relatórios. O comportamento é idêntico nos dois contextos.

**Paginação no servidor.** Cada página é seu próprio `SELECT *` governado com `LIMIT 101 OFFSET n`. São exibidas 100 linhas por página; a 101ª sinaliza se existem mais. O conjunto de dados completo nunca é carregado no navegador. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtros e ordenações empurrados para a origem.** Cada cabeçalho de coluna tem um campo de filtro. Termos de filtro tornam-se predicados `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; cliques de ordenação produzem cláusulas `ORDER BY`. Ambos são enviados ao banco de dados — filtrar uma tabela de um bilhão de linhas varre a origem, não as 100 linhas à sua frente. [tool-verified: `nativeParams.ts:53-70`]

**Agrupamento multinível.** O ícone de camadas em cada cabeçalho de coluna insere aquela coluna no agrupamento. As colunas de agrupamento lideram o `ORDER BY`, de modo que os membros de um grupo caem na mesma página que seu cabeçalho, mesmo através das fronteiras de página. Colunas de chave primária são anexadas ao final como critério de desempate estável. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Linhas de cabeçalho de grupo são recolhíveis; recolher esconde os membros sem emitir uma nova consulta. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**As escolhas persistem.** Configurações de filtro, ordenação e agrupamento são salvas em `localStorage` sob `provisa.grid.table:<domain>.<table>` e restauradas na visita seguinte. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Exportação.** Baixe a página atual como CSV ou copie-a para a área de transferência como texto separado por tabulações. A exportação cobre apenas a página visível. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
