# API de Administração

A API de administração é um endpoint GraphQL Strawberry em `POST /admin/graphql` (REQ-533). Ela exige uma função de superusuário ou administrador (REQ-125, REQ-060) e é separada do endpoint GraphQL de dados (REQ-533).

## Autenticação

Passe suas credenciais no cabeçalho `Authorization` usando o provedor de autenticação padrão do Provisa (REQ-120):

```yaml
Authorization: Bearer <token>
```

O acesso administrativo é governado pela capacidade `admin` atribuída a uma função (REQ-060, REQ-042).

### Tokens de acesso pessoal

Um token de acesso pessoal é aceito em qualquer lugar onde um token bearer seja, inclusive neste endpoint. Emitir e revogar um token é autoatendimento — trata-se da credencial do próprio portador, então ela fica no perfil do usuário na interface de administração, e não sob uma página de administrador, ao lado de sair de uma organização e excluir a conta. Um administrador não emite tokens em nome de outra pessoa. (REQ-1263)

| Rota | Efeito |
| ------- | -------- |
| `POST /auth/tokens` | Emite um token para quem chama. Corpo: `name`, opcionalmente `role_id`, `scopes`, `expires_in_days` (1–366). A resposta é o único lugar em que o segredo aparece |
| `GET /auth/tokens` | Os tokens ativos de quem chama nesta organização — prefixo de exibição, nome, marcações de ciclo de vida e o hash que identifica um token para revogação. Nunca uma credencial funcional |
| `DELETE /auth/tokens/{token_hash}` | Revoga um dos tokens de quem chama. 404 quando o token não é dele ou já foi revogado |

Omitir `role_id` deixa o token resolvendo para a função que seu dono tiver; nomear uma função restringe o token abaixo do seu dono. A revogação também acontece de forma implícita: remover a associação de um usuário a uma organização revoga os tokens dele naquela organização. Veja [Modelo de Segurança](security.md#tokens-de-acesso-pessoal) para a credencial em si.

## Capacidades

### Gerenciamento de configuração

Baixe a configuração em execução (REQ-164):

```http
GET /admin/config
```

Retorna o `config.yaml` completo como arquivo YAML. Envie uma nova configuração (REQ-164):

```http
PUT /admin/config
```

O Provisa valida o YAML, recarrega os catálogos e regenera os esquemas (REQ-012, REQ-253). Nenhuma reinicialização necessária.

### Configurações de runtime

Leia e grave configurações de plataforma em runtime sem editar o arquivo de configuração (REQ-165):

```http
GET  /admin/settings
PUT  /admin/settings
```

A interface de configurações cobre redirecionamento de resultados grandes, amostragem e limite de linhas padrão, TTL do cache de resposta, convenção de nomenclatura, rastreamento automático de FK de relacionamento, DSN do armazenamento de materialização, memória do motor de federação (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e toda a interface de ajuste do pipeline de rastreamento OpenTelemetry (REQ-1082). Limites de travessia de GraphQL remoto e configurações de camada morna/cache de leitura também são expostos (REQ-1081, REQ-1083).

Postura de segurança — `security.mode` (`standard` | `high`) — aplicada na reinicialização (REQ-1079):

```http
GET  /admin/security
PUT  /admin/security
```

Atribuições de modelo de IA, o registro de modelos de embedding/vetor e o limite de taxa de NL — entram em vigor na próxima requisição, sem reinicialização (REQ-1349): [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

A aba de criptografia da administração deriva sua lista de provedores ao vivo do registro de criptografia; provedores indisponíveis aparecem, mas não são selecionáveis (REQ-1091).

`GET`/`HEAD /health` e `GET /setup/status` são sempre não autenticados — eles ignoram a exigência de `Authorization: Bearer` mesmo quando um provedor de autenticação está configurado (REQ-539).

### Motor de federação

Leia ou altere qual motor a implantação usa (REQ-916):

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` retorna a chave do motor ativo e os campos de configuração de que ele precisa. `PUT` aceita um corpo com `engine` (a chave) e quaisquer campos específicos do motor; a seleção é persistida na configuração da plataforma e passa a valer na próxima reinicialização do serviço. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

### Editor de relacionamentos

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

Declare um relacionamento apoiado em junção (REQ-1586):

```graphql
mutation {
  upsertRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

Uma tabela associativa é declarada como aresta, nunca descoberta. `viaTable` nomeia uma tabela registrada; suas duas colunas-chave carregam a aresta, e cada coluna restante vira um atributo do relacionamento, filtrável como qualquer outro campo. `viaTypeColumn` / `viaTypeValue` dividem uma mesma tabela de junção em vários tipos de aresta — três linhas de `pet_companions` com `companion_type` igual a `bonded pair`, `littermate` e `shares enclosure` são três relacionamentos distintos sobre o mesmo par de tabelas.

`viaLabelSource` indica de onde vem o nome exposto, e as três formas são convertidas para UPPER_SNAKE_CASE no Cypher: `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa o nome da própria tabela de junção (`PET_COMPANIONS`), `fixed` usa o `alias` declarado. Uma tabela de junção declarada assim é uma aresta e não uma entidade — ela sai dos rótulos de nó, então nunca aparece como pílula de nó na UI do grafo. [tool-verified: `provisa/api/admin/types.py:606-611`, `provisa/api/admin/db_queries.py:47-82`]

### Descoberta de relacionamentos por IA

Dispare a análise de FK movida pelo Claude via REST (REQ-167, REQ-018):

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Retorna candidatos a FK ordenados por confiança. Aceite um candidato:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspecção de esquema

Navegue pelas tabelas publicadas em todas as fontes (REQ-008):

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

### Verificação de dependências de coluna (REQ-1484)

Antes de salvar a edição de uma tabela que renomeia o alias SQL de uma coluna ou remove uma coluna, pergunte o que mais
a referencia:

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renomear um alias quebra todo artefato escrito contra o nome exposto — exibições, MVs, expressões de
métrica, predicados de RLS, contratos de DQ. Remover uma coluna quebra esses e mais os artefatos que
armazenam o `column_name` físico: relacionamentos, vínculos de glossário, atribuições de tag. `breaksOn`
diz qual é o caso. A página Tabelas executa isso ao salvar e mostra o resultado como um diálogo informativo. Veja
[Lineage](lineage.md) para o que a consulta cobre e o que ela não alcança.

### Gerenciamento de exibições

Registre uma exibição materializada (REQ-133, REQ-135):

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

### Registro de fontes de grafo

Fontes Neo4j e SPARQL são registradas por endpoints REST (não pela API GraphQL de administração) (REQ-295, REQ-297):

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

Uma vez registradas, as tabelas aparecem no esquema GraphQL e são consultáveis como qualquer outra fonte (REQ-016).

### Importação Hasura / DDN (REQ-1483)

Converta um projeto Hasura v2 ou Hasura DDN existente em configuração do Provisa pela interface de administração ou pela API, sem que nada seja aplicado até você aprovar.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**Preview** converte o arquivo enviado e retorna o `config_yaml` proposto, uma lista de avisos e um resumo do que foi encontrado (contagens de fonte, domínio, tabela, coluna, função, relacionamento e RLS). Nada é gravado no banco de dados do inquilino. Corpo da requisição:

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` é `"auto"` (detectado pela estrutura do arquivo), `"hasura_v2"` ou `"ddn"`.

**Apply** toma o YAML que você revisou (e opcionalmente editou) e o carrega na organização atuante — o mesmo caminho de recarga a quente de `PUT /admin/config`. Corpo da requisição: `{"config_yaml": "<yaml string>"}`.

O preview nunca guarda em cache o YAML convertido no servidor; o apply toma o YAML que você fornece, então o que é aplicado é exatamente o que foi revisado. [tool-verified: `provisa/api/admin/import_router.py`]

### Intercâmbio com o Apache Ossie (REQ-1316, REQ-1321)

O Provisa interopera com o Apache Ossie (incubating) como fronteira de importação/exportação.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Exportação** (`GET /admin/ossie`) deriva o documento YAML do Ossie a partir do modelo governado ao vivo a cada requisição — ele nunca é armazenado em cache, portanto não pode ficar desatualizado. A resposta é `text/yaml` com um cabeçalho `Content-Disposition: attachment`. Tabelas viram objetos `dataset`, colunas viram objetos `field` e relacionamentos mapeiam para objetos `relationship` do Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**Importação** (`POST /admin/ossie/import`) aceita um documento YAML ou JSON do Ossie (o formato é detectado automaticamente). Ela analisa o documento e retorna registros propostos de tabela e relacionamento como um objeto JSON — nada é registrado. A tela de revisão na interface de administração permite aceitar ou reduzir as propostas antes que qualquer mutação dispare. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Armazenamento de objetos (REQ-1046, REQ-1048, REQ-1049)

Leia ou configure o armazenamento de materialização da organização:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` informa quanto da cota de armazenamento da plataforma a organização usa. `PUT` registra o DSN de armazenamento da própria organização (criptografado em repouso; nunca retornado pelo GET). Uma vez definido, as materializações da organização passam a ir para o bucket dela e deixam de contar contra a cota da plataforma. Enviar `storage_url: null` limpa o valor e devolve a organização ao armazenamento da plataforma. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Criptografia da organização (REQ-1574)

Defina ou rotacione a chave de criptografia em repouso da organização:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` retorna a impressão digital da chave, seu id e sua procedência — nunca o material da chave. `PUT` define ou rotaciona a chave. Forneça `key_b64` (32 bytes brutos, codificados em base64) para trazer sua própria chave, ou omita-o para que o Provisa gere uma. Não há exclusão: aposentar a última chave deixaria ilegível todo payload que ela envolveu. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

A API de administração acompanha o GraphiQL em `GET /admin/graphql` no navegador (REQ-622). Use-o para explorar o esquema administrativo completo de forma interativa.

## Exibições de gerenciamento do domínio ops (REQ-1386)

Oito exibições SQL são semeadas no domínio `ops` built-in em toda instalação. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Elas expõem o log de auditoria de consultas como tabelas governadas — consultáveis por SQL (pgwire), GraphQL e Cypher sob as mesmas regras de acesso a domínio, RLS e mascaramento de qualquer tabela de negócio.

`org_admin` é designado como o responsável pelo domínio ops no momento da semeadura, de modo que o domínio nunca apareça como lacuna de governança em `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Exibição | O que ela responde |
| --- | --- |
| `usage_ranking` | Contagem de consultas e usuários distintos por tabela registrada; tabelas sem nenhum acesso surgem como candidatas a descontinuação |
| `deprecated_usage` | Todo acesso a uma tabela ou coluna com a tag `deprecated` — os consumidores ativos que bloqueiam uma remoção segura |
| `pii_access` | Todo acesso a uma tabela ou coluna com a tag `pii`: quem consultou, sob qual função, por qual interface |
| `policy_denials` | Todas as tentativas de acesso que a governança rejeitou (HTTP 401/403) |
| `surface_mix` | Contagem diária de consultas e usuários distintos por interface de protocolo (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Contagem diária de erros e latência média/máxima por interface |
| `stale_metadata` | Tabelas e colunas sem descrição; domínios sem responsável |
| `join_hotspots` | Pares de tabelas coconsultadas com mais frequência — candidatas a materialização ou cache |

Dois limites se aplicam hoje. A granularidade é no nível de tabela — o log de auditoria registra `table_ids`, não as colunas individuais acessadas. O texto da consulta é criptografado (REQ-689) e excluído de todas as exibições aqui; ele só é acessível pelo caminho autorizado de descriptografia administrativa. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Uma função precisa de acesso ao domínio `ops` antes que essas exibições fiquem visíveis. Conceda-o do mesmo modo que você concede acesso a qualquer outro domínio.

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

As mesmas consultas rodam como GraphQL ou Cypher sobre qualquer transporte governado — pgwire, Arrow Flight ou Bolt. [inferred from governed-surface design]

## Visualizador de relatórios (REQ-1390)

O visualizador de relatórios fica em `/admin/reports`. Funções sem a capacidade `observability` não conseguem alcançá-lo.

O painel esquerdo lista toda tabela registrada no domínio `ops`, ordenada por alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] As oito exibições de gerenciamento semeadas aparecem ali automaticamente. Clique em qualquer relatório para carregá-lo no visualizador de dados governados à direita.

**Adicionar um relatório personalizado.** O botão "Adicionar relatório" abre um diálogo. Informe um nome, uma descrição opcional e uma instrução SELECT. Salvar registra a exibição como tabela derivada governada no domínio `ops` — catalogada, com controle de acesso e consultável por todas as interfaces ao lado das exibições semeadas. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Excluir.** O ícone de lixeira aparece apenas para relatórios personalizados. Exibições de gerenciamento semeadas não podem ser excluídas por esta interface. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Pré-visualização de tabela (REQ-1392)

Expanda qualquer linha de tabela na página Tabelas. O botão **Pré-visualizar** abre um modal de 90% de largura com os dados governados ao vivo da tabela. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Tabelas apoiadas em APIs com parâmetros de caminho obrigatórios bloqueiam a pré-visualização até que esses valores sejam fornecidos. Um formulário embutido coleta cada parâmetro obrigatório antes da primeira consulta; parâmetros de consulta opcionais aparecem no mesmo formulário. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visualizador de dados governados (REQ-1391)

O mesmo componente de visualização movimenta o modal de pré-visualização e o visualizador de relatórios. Seu comportamento é idêntico nos dois contextos.

**Paginação no servidor.** Cada página é seu próprio `SELECT *` governado com `LIMIT 101 OFFSET n`. 100 linhas aparecem por página; a 101ª sinaliza se há mais. O conjunto de dados completo nunca é carregado no navegador. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtros e ordenações empurrados para baixo.** Cada cabeçalho de coluna tem um campo de filtro. Termos de filtro viram predicados `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`; cliques de ordenação produzem cláusulas `ORDER BY`. Ambos vão para o banco de dados — um filtro em uma tabela de um bilhão de linhas varre a fonte, não a página de 100 linhas à sua frente. [tool-verified: `nativeParams.ts:53-70`]

**Agrupamento em múltiplos níveis.** O ícone de Camadas em qualquer cabeçalho de coluna alterna aquela coluna no agrupamento. Colunas de agrupamento lideram o `ORDER BY` para que os membros de um grupo caiam na mesma página do seu cabeçalho ao cruzar limites de página. Colunas de chave primária são anexadas como critério de desempate estável. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Linhas de cabeçalho de grupo são recolhíveis; recolher esconde os membros sem emitir uma nova consulta. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Escolhas persistentes.** Configurações de filtro, ordenação e agrupamento persistem em `localStorage` sob `provisa.grid.table:<domain>.<table>` e são restauradas na próxima visita. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Exportação.** Baixe a página atual como CSV ou copie-a para a área de transferência como texto separado por tabulações. A exportação cobre apenas a página visível. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
