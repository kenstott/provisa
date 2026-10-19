# API de Administração {: #admin-api }

A API de administração é um endpoint GraphQL Strawberry em `POST /admin/graphql`. Ela exige uma função de superusuário ou administrador e é separada do endpoint GraphQL de dados.

## Autenticação {: #authentication }

Passe suas credenciais no cabeçalho `Authorization` usando o provedor de autenticação padrão do Provisa:

```yaml
Authorization: Bearer <token>
```

O acesso administrativo é governado pela capacidade `admin` atribuída a uma função.

### Tokens de acesso pessoal {: #personal-access-tokens }

Um token de acesso pessoal é aceito em qualquer lugar onde um token bearer é aceito, incluindo este endpoint. Você emite e revoga os seus, porque um token é a credencial do próprio titular. Por isso ele fica no seu perfil na UI de administração, ao lado de sair de uma organização e excluir a conta, e não numa página de administração. Um administrador nunca emite um token em nome de outra pessoa.

| Rota | Efeito |
| ------- | -------- |
| `POST /auth/tokens` | Emite um token para quem chama. Corpo: `name`, opcionalmente `role_id`, `scopes`, `expires_in_days` (1–366). A resposta é o único lugar em que o segredo aparece |
| `GET /auth/tokens` | Os tokens ativos de quem chama nesta organização — prefixo de exibição, nome, marcações de ciclo de vida e o hash que identifica um token para revogação. Nunca uma credencial funcional |
| `DELETE /auth/tokens/{token_hash}` | Revoga um dos tokens de quem chama. 404 quando o token não é dele ou já foi revogado |

Omitir `role_id` deixa o token resolvendo para a função que seu dono tiver; nomear uma função restringe o token abaixo do seu dono. A revogação também acontece de forma implícita: remover a associação de um usuário a uma organização revoga os tokens dele naquela organização. Veja [Modelo de Segurança](security.md#personal-access-tokens) para a credencial em si.

## Capacidades {: #capabilities }

### Gerenciamento de configuração {: #config-management }

Baixe a configuração em execução:

```http
GET /admin/config
```

Retorna o `config.yaml` completo como arquivo YAML. Envie uma nova configuração:

```http
PUT /admin/config
```

O Provisa valida o YAML, recarrega os catálogos e regenera os esquemas. Nenhuma reinicialização necessária.

### Configurações de runtime {: #runtime-settings }

Leia e grave configurações de plataforma em runtime sem editar o arquivo de configuração:

```http
GET  /admin/settings
PUT  /admin/settings
```

A interface de configurações cobre redirecionamento de resultados grandes, amostragem e limite de linhas padrão, TTL do cache de resposta, convenção de nomenclatura, rastreamento automático de FK de relacionamento, DSN do armazenamento de materialização, memória do motor de federação (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) e toda a interface de ajuste do pipeline de rastreamento OpenTelemetry. Limites de travessia de GraphQL remoto e configurações de camada morna/cache de leitura também são expostos.

O cartão **Servidores de conectores** nomeia, para cada conector com um servidor incluído (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), uma pasta contendo um pacote de servidor construído para este host (`connectors.<connector>.bundle_dir`, variável de ambiente `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Vazio, o servidor do conector é obtido da versão fixada de `kenstott/calcite`. Definido, a pasta é usada como está: seu launcher deve estar em `bin/pgwire-<connector>`, e uma pasta sem ele impede o servidor do conector de iniciar com um erro nomeando a configuração; a versão nunca é obtida em seu lugar. A configuração é lida a cada vez que o servidor de um conector inicia.

A postura de segurança, `security.mode` (`standard` | `high`), é aplicada ao reiniciar:

```http
GET  /admin/security
PUT  /admin/security
```

As atribuições de modelos de IA, o registro de modelos de embedding e vetoriais e o limite de taxa de NL valem a partir da próxima requisição, sem reiniciar:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

A aba de criptografia da administração deriva sua lista de provedores ao vivo do registro de criptografia; provedores indisponíveis aparecem, mas não são selecionáveis.

`GET`/`HEAD /health` e `GET /setup/status` nunca exigem autenticação. Eles pulam a exigência de `Authorization: Bearer` mesmo com um provedor de autenticação configurado.

### Motor de federação {: #federation-engine }

Leia ou altere qual motor a implantação usa:

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` retorna a chave do motor ativo e os campos de configuração de que ele precisa. `PUT` aceita um corpo com `engine` (a chave) e quaisquer campos específicos do motor; a seleção é persistida na configuração da plataforma e passa a valer na próxima reinicialização do serviço.

### Editor de relacionamentos {: #relationship-editor }

Liste relacionamentos:

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

Criar um relacionamento. Um id que já existe é recusado; um relacionamento é alterado com `updateRelationship`, que recebe a mesma entrada:

```graphql
mutation {
  createRelationship(input: {
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

Declare um relacionamento apoiado em junção:

```graphql
mutation {
  createRelationship(input: {
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

Você declara uma tabela associativa como aresta; o Provisa nunca descobre uma. `viaTable` nomeia uma tabela registrada. Suas duas colunas-chave carregam a aresta, e cada coluna restante vira um atributo do relacionamento, filtrável como qualquer campo. `viaTypeColumn` / `viaTypeValue` dividem uma tabela de junção em vários tipos de aresta. Por exemplo, linhas de `pet_companions` cujo `companion_type` é `bonded pair`, `littermate` ou `shares enclosure` dão três relacionamentos distintos sobre o mesmo par de tabelas.

`viaLabelSource` indica de onde vem o nome exposto, e o Cypher recebe cada forma em maiúsculas com sublinhados. `column` usa `viaTypeValue` (`BONDED_PAIR`), `table` usa o nome da própria tabela de junção (`PET_COMPANIONS`) e `fixed` usa o `alias` declarado. Uma tabela de junção declarada assim é uma aresta. O Provisa a tira dos rótulos de nó, então ela nunca aparece como pílula de nó na UI do grafo.

### Descoberta de relacionamentos por IA {: #ai-relationship-discovery }

Dispare a análise de FK movida pelo Claude via REST:

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

### Introspecção de esquema {: #schema-introspection }

Navegue pelas tabelas publicadas em todas as fontes:

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Verificação de dependências de coluna {: #column-dependency-check }

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

Renomear um alias quebra todo artefato escrito contra o nome exposto: views, MVs, expressões de métricas, predicados de RLS e contratos de DQ. Remover uma coluna quebra esses também, mais os artefatos que guardam o `column_name` físico, que são relacionamentos, vínculos do glossário e atribuições de tags. `breaksOn` diz qual. A página de tabelas roda essa verificação ao salvar e mostra o resultado num diálogo de aviso. [Lineage](lineage.md) explica o que a consulta cobre e o que não cobre.

### Gerenciamento de exibições {: #view-management }

Registre uma exibição materializada:

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

Dispare uma atualização manual:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Registro de fontes de grafo {: #graph-source-registration }

Fontes Neo4j e SPARQL são registradas por endpoints REST (não pela API GraphQL de administração):

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

Uma vez registradas, as tabelas aparecem no esquema GraphQL e são consultáveis como qualquer outra fonte.

### Importação Hasura / DDN {: #hasura-ddn-import }

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

**Apply** carrega o YAML que você revisou, e editou se quis, na organização que age. Usa o mesmo caminho de recarga a quente que `PUT /admin/config`. Corpo da requisição: `{"config_yaml": "<yaml string>"}`.

O preview nunca guarda em cache o YAML convertido no servidor; o apply toma o YAML que você fornece, então o que é aplicado é exatamente o que foi revisado.

### Intercâmbio com o Apache Ossie {: #apache-ossie-interchange }

O Provisa interopera com o Apache Ossie (incubating) como fronteira de importação/exportação.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Exportação** (`GET /admin/ossie`) monta o documento YAML do Ossie a partir do modelo governado ativo a cada requisição. Nada fica em cache, então o documento nunca está desatualizado. A resposta é `text/yaml` com um cabeçalho `Content-Disposition: attachment`. O documento é um modelo com `version`, `name`, `datasets`, `relationships` e `metrics` na raiz. Tabelas viram objetos `dataset`, colunas viram objetos `field`, e relacionamentos correspondem a objetos `relationship` do Ossie. Um modelo sem tabela é recusado com `422`.

**Importação** (`POST /admin/ossie/import`) aceita um documento Ossie em YAML ou JSON e detecta o formato sozinha. Ela analisa o documento e devolve, como objeto JSON, registros propostos de tabelas, relacionamentos e métricas. Nada é registrado. Ela lê só a forma plana do documento, e um documento que envolve seu modelo em `semantic_model` é recusado pelo nome. Na UI de administração, a tela de revisão deixa você aceitar ou cortar propostas antes que algo mude.

### Armazenamento de objetos {: #object-storage }

Leia ou configure o armazenamento de materialização da organização:

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` informa quanto da cota de armazenamento da plataforma a organização usa. `PUT` registra o DSN de armazenamento da própria organização (criptografado em repouso; nunca retornado pelo GET). Uma vez definido, as materializações da organização passam a ir para o bucket dela e deixam de contar contra a cota da plataforma. Enviar `storage_url: null` limpa o valor e devolve a organização ao armazenamento da plataforma.

### Criptografia da organização {: #org-encryption }

Defina ou rotacione a chave de criptografia em repouso da organização:

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` devolve a impressão digital, o id e a procedência da chave, nunca material de chave. `PUT` define ou rotaciona a chave. Informe `key_b64` (32 bytes brutos, codificados em base64) para trazer sua própria chave, ou omita e o Provisa gera uma. Não há exclusão, porque aposentar a última chave deixaria ilegível cada payload que ela envolveu.

## GraphiQL

A API de administração acompanha o GraphiQL em `GET /admin/graphql` no navegador. Use-o para explorar o esquema administrativo completo de forma interativa.

## Exibições de gerenciamento do domínio ops {: #ops-domain-management-views }

Toda instalação semeia oito views SQL no domínio embutido `ops`. Elas expõem o log de auditoria de consultas como tabelas governadas. Você as consulta por SQL (pgwire), GraphQL e Cypher, sob as mesmas regras de acesso ao domínio, RLS e mascaramento de qualquer tabela de negócio.

`org_admin` é designado como o responsável pelo domínio ops no momento da semeadura, de modo que o domínio nunca apareça como lacuna de governança em `stale_metadata`.

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

A granularidade para na tabela. O log de auditoria registra `table_ids`, e as colunas que uma consulta leu não são registradas. O texto das consultas é cifrado e fica fora de todas estas views; só o caminho autorizado de decifração da administração consegue lê-lo.

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

Como as views são tabelas governadas, você pode escrever as mesmas consultas em GraphQL ou Cypher.

## Visualizador de relatórios {: #reports-viewer }

O visualizador de relatórios fica em `/admin/reports`. Funções sem a capacidade `observability` não conseguem alcançá-lo.

O painel esquerdo lista toda tabela registrada no domínio `ops`, ordenada por alias. As oito exibições de gerenciamento semeadas aparecem ali automaticamente. Clique em qualquer relatório para carregá-lo no visualizador de dados governados à direita.

**Adicionar um relatório personalizado.** O botão "Add report" abre um diálogo onde você informa um nome, uma descrição opcional e uma instrução SELECT. Salvar registra a view como tabela derivada governada no domínio `ops`. Ela fica catalogada, com controle de acesso e consultável por qualquer superfície, junto às views semeadas.

**Excluir.** O ícone de lixeira aparece apenas para relatórios personalizados. Exibições de gerenciamento semeadas não podem ser excluídas por esta interface.

## Pré-visualização de tabela {: #table-preview }

Expanda qualquer linha de tabela na página Tabelas. O botão **Pré-visualizar** abre um modal de 90% de largura com os dados governados ao vivo da tabela.

Tabelas apoiadas em APIs com parâmetros de caminho obrigatórios bloqueiam a pré-visualização até que esses valores sejam fornecidos. Um formulário embutido coleta cada parâmetro obrigatório antes da primeira consulta; parâmetros de consulta opcionais aparecem no mesmo formulário.

## Visualizador de dados governados {: #governed-data-viewer }

O mesmo componente de visualização movimenta o modal de pré-visualização e o visualizador de relatórios. Seu comportamento é idêntico nos dois contextos.

**Paginação no servidor.** Cada página é seu próprio `SELECT *` governado com `LIMIT 101 OFFSET n`. 100 linhas aparecem por página; a 101ª sinaliza se há mais. O conjunto de dados completo nunca é carregado no navegador.

**Filtros e ordenações empurrados para baixo.** Cada cabeçalho de coluna tem um campo de filtro. Um termo de filtro vira um predicado `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, e um clique de ordenação produz uma cláusula `ORDER BY`. Os dois rodam no banco de dados. Um filtro numa tabela de um bilhão de linhas varre a fonte, e seu alcance nunca se limita à página de 100 linhas à sua frente.

**Agrupamento em múltiplos níveis.** O ícone de Camadas em qualquer cabeçalho de coluna alterna aquela coluna no agrupamento. Colunas de agrupamento lideram o `ORDER BY` para que os membros de um grupo caiam na mesma página do seu cabeçalho ao cruzar limites de página. Colunas de chave primária são anexadas como critério de desempate estável. Linhas de cabeçalho de grupo são recolhíveis; recolher esconde os membros sem emitir uma nova consulta.

**Escolhas persistentes.** Configurações de filtro, ordenação e agrupamento persistem em `localStorage` sob `provisa.grid.table:<domain>.<table>` e são restauradas na próxima visita.

**Exportação.** Baixe a página atual como CSV ou copie-a para a área de transferência como texto separado por tabulações. A exportação cobre apenas a página visível.
