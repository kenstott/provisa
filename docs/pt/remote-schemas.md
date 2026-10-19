# Esquemas Remotos (Remote Schemas) {: #remote-schemas }

Uma fonte de esquema remoto conecta uma API externa, seja GraphQL (incluindo o GitHub), gRPC ou REST (OpenAPI), ao modelo do Provisa. Adicionar a fonte não registra nenhuma tabela. A fonte oferece tabelas, e um steward registra cada uma que quiser no seletor Register Table; esse registro é a etapa de curadoria. Uma tabela registrada é uma tabela do Provisa completa, então toda regra de governança, interface de consulta e camada de segurança se aplica a ela automaticamente. O serviço remoto nunca vê as regras de governança do Provisa.

---

## Três tipos de fonte {: #three-source-types }

### Esquema remoto GraphQL {: #graphql-remote-schema }

**Como adicionar a fonte.** Faça POST para `/admin/sources/graphql-remote` com a URL do endpoint, um namespace, e auth opcional. O Provisa dispara uma consulta de introspecção `__schema` padrão contra o endpoint remoto para confirmar o endpoint e a credencial.

Adicionar a fonte não registra nenhuma tabela nem nenhum command. Todo tipo de fonte remota responde à adição e à atualização com os mesmos contadores: `tables` (tabelas registradas ou atualizadas; 0 na adição), `available_tables` (tabelas oferecidas), `mutations` (sempre 0) e `available_mutations` (commands oferecidos).

**Registrar tabelas.** Abra Tables, depois Register Table, escolha a fonte e o esquema `graphql`, e selecione as tabelas e colunas desejadas. Pela API GraphQL de administração: `availableTables(sourceId, schemaName)` lista as tabelas oferecidas, `availableColumns` lista as colunas de uma tabela, e `registerTable(input: TableInput)` registra uma com as colunas escolhidas. Uma tabela registrada passa então a ser governada.

Como uma tabela registrada é lida (campo raiz, caminho das linhas, argumentos obrigatórios, argumentos de paginação) fica armazenado em `sources.mapping["tables"]`, de modo que um processo reiniciado a lê sem pedir ao remoto o seu esquema.

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

Opções de auth: `none`, `bearer` (header Authorization), `basic` (Base64 username:password).

**Overrides de campo.** `field_overrides` é um mapa `{fieldName: "query" | "mutation"}` aplicado após a introspecção. Tem prioridade sobre a classificação estrutural. Somente campos do tipo query podem ser reclassificados como mutations; campos do tipo mutation não têm caminho de override no GraphQL.

**Relacionamentos no momento do registro.** `relationships` declara caminhos de join FK/PK entre tabelas no momento do registro. Estes são armazenados como relacionamentos declarados manualmente (sem flag `remote_managed`). Na atualização, relacionamentos auto-detectados (aqueles com `remote_managed: True`) são reexecutados e podem mudar; relacionamentos declarados manualmente não são tocados.

**O que a fonte oferece.** Todo campo do tipo `Query` remoto que retorna um objeto ou uma lista de objetos é oferecido como tabela, e também cada conexão Relay sob um campo de objeto único (veja abaixo). Registrar uma tabela oferecida a torna uma tabela. Cada campo do tipo `Mutation` remoto é um command oferecido, contado em `available_mutations`; adicionar a fonte não registra nenhum. Registre os que quiser como commands; veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation).

**Nomeação de tabela.** Tabelas são nomeadas `{namespace}__{field_name}`. Com namespace `petstore` e um campo de consulta `pets`: o nome da tabela é `petstore__pets`.

**Conexões Relay.** Muitas APIs retornam listas como conexões Relay: um objeto com `nodes` (ou `edges { node }`) ao lado de `pageInfo`. O Provisa mapeia uma conexão para uma tabela dos seus nós e a lê página por página.

- Um campo raiz que retorna uma conexão (`securityAdvisories`) vira uma tabela dos seus nós.
- Uma conexão no objeto único que um campo raiz retorna vira sua própria tabela. A tabela recebe os argumentos obrigatórios do campo raiz. Com `repository(owner, name)` e uma conexão `issues` em `Repository`, a tabela é `repositoryIssues`, com nome SQL `gh__repository_issues` sob o namespace `gh`. Filtre-a pelas colunas `_nf_owner` e `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Uma conexão nunca é uma coluna. Caso contrário, uma linha carregaria uma leitura que o remoto calcula por linha, para cada conexão que o seu tipo tem.
- Uma conexão só é tabela se o seu campo aceita `first` e `after`, de modo que possa ser lida página por página. Uma que precisa de um argumento próprio não é tabela. Também não é uma conexão de uma união, nem qualquer conexão sob um campo raiz que retorna uma lista.

**Mapeamento de tipo.** Campos escalares mapeiam diretamente para tipos Provisa. Campos OBJECT se dividem em dois casos dependendo se o tipo alvo é governado (veja "Tabelas governadas" abaixo).

| Tipo GraphQL | Tipo Provisa |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (tipo inline não governado, ex.: `ContactInfo`) | coluna blob `jsonb` |
| OBJECT (tipo alvo governado) | excluído inteiramente da SDL e da busca |
| Qualquer ENUM | `jsonb` |
| Escalar customizado | `text` (padrão) |

**Tabelas governadas.** Um tipo GQL é governado quando aparece como um campo raiz `Query` no esquema remoto. `_collect_queryable_types` coleta estes durante o registro, preferindo campos sem argumento obrigatório para que possam ser buscados em massa como alvos de join.

Quando uma coluna do tipo OBJECT em uma tabela governada aponta para outro tipo governado, essa coluna está sujeita a três regras simultaneamente:

1. **Excluída da busca GQL** — o campo não é solicitado ao buscar linhas da tabela pai.
2. **Excluída da SDL** — o campo não aparece no tipo pai no esquema gerado.
3. **Acessível somente via um relacionamento declarado** — um steward deve registrar um JOIN entre as duas tabelas governadas materializadas. Sem um, o campo simplesmente está ausente; não há fallback de blob.

Tipos OBJECT que NÃO são alcançáveis como campos Query raiz (tipos inline como `ContactInfo` ou `Address`) seguem regras diferentes: são buscados como colunas blob `jsonb` e aparecem na SDL como campos de objeto aninhado. Sub-campos são acessíveis via extração `->>` em SQL.

**Campos que precisam de um argumento não são colunas.** Um campo com um argumento obrigatório não pode ser selecionado diretamente, então fica de fora das colunas da tabela e das seleções aninhadas.

**Argumentos obrigatórios.** Quando um campo de consulta raiz tem argumentos non-null sem valor padrão, esses se tornam colunas `native_filter_type: query_param` na tabela (prefixadas com `_nf_` no momento da injeção). O executor as passa como variáveis GraphQL.

**Relacionamentos detectados automaticamente.** O Provisa examina as colunas do tipo OBJECT de cada tabela registrada. Quando o tipo GQL a que uma coluna se refere também é uma tabela registrada da mesma fonte, e a coluna em que o relacionamento se apoia está entre as colunas registradas, o relacionamento é salvo. Uma tabela ainda não registrada não recebe nenhum. Relacionamentos many-to-one deduzem as colunas de origem e destino das convenções de nomes (`breedName` no tipo de origem → `name` no tipo de destino `Breed`). Campos one-to-many (LIST) geram relacionamentos com referências de coluna vazias, porque a FK fica do lado do destino.

**Mutações.** Um campo de mutação é registrado, um a um, como um command do tipo `source_operation`. Seus argumentos são cada um tipado como `json` e repassados ao serviço remoto como variáveis tipadas; a resposta é o JSON que o serviço remoto retorna, sem `return_schema`. Veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation).

**Atualização (refresh).** Faça POST para `/admin/sources/graphql-remote/{id}/refresh`. Reintrospecciona o esquema remoto e atualiza, conforme ele, as tabelas já registradas. Não adiciona nenhuma tabela nem coluna: uma tabela ou coluna que o esquema ganhou continua oferecida, e uma coluna que o esquema perdeu é removida. Regras de governança existentes (RLS, mascaramento) são preservadas.

**Limitações.**

- Campos de consulta raiz escalares e ENUM (tipo de retorno não é OBJECT) se tornam funções rastreadas, não tabelas virtuais. Seu `return_schema` é uma única coluna `value` do tipo escalar mapeado.
- O aninhamento de objeto é resolvido no momento do registro até `graphql_remote.max_object_depth` (padrão: 5). Tanto a seleção da busca remota quanto os metadados dos sub-campos são construídos até essa profundidade; campos além do limite não são buscados e não estão disponíveis para extração em SQL. Um tipo é visitado uma só vez ao longo de qualquer caminho: um campo cujo tipo já está no caminho descendente é deixado de fora, de modo que um esquema cujos tipos se referem uns aos outros é percorrido uma vez por tipo, não uma vez por nível de profundidade.
- Campos OBJECT aninhados do tipo LIST (ex.: `breed.awards: [Award]`) são incluídos na seleção de busca até `graphql_remote.max_list_depth` níveis de aninhamento (padrão: 2). Dentro desse limite, a lista é buscada como um array `jsonb` na coluna pai. Quando o campo de lista declara um argumento `first` (Relay, PostGraphile, pg_graphql) ou um argumento `limit` (Hasura), a seleção o passa como `first: N` ou `limit: N`, em que N é `graphql_remote.max_list_items` (padrão: 100). Um campo de lista que não declara nenhum dos dois não recebe argumento, porque um remoto rejeita um argumento que o campo não declara. Além de `max_list_depth`, o campo LIST é excluído por completo para evitar expansão ilimitada de dados. Em SQL, o array é acessado via `json_array_elements(column_name)` ou extração por índice com `->>`. Se o tipo de item da lista tiver sua própria consulta raiz, registre-o como uma tabela separada e crie um relacionamento. O caminho de join é mais eficiente e contorna o blob.
- Para consultas SQL, colunas do tipo OBJECT não governadas são buscadas por completo do remoto (todos os sub-campos até a profundidade configurada) e cacheadas como `jsonb`. O acesso a sub-campo em SQL é tratado via extração `->>` contra o blob; a requisição remota não é reduzida somente aos campos que a consulta SQL seleciona. Quando o tipo de item da LIST não tem consulta raiz e a representação em blob é insuficiente, escreva a consulta em SDL GraphQL diretamente. O Provisa reproduz fielmente a seleção de campo GQL, para que o remoto veja exatamente os campos solicitados.
- Se o servidor remoto rejeitar um campo do tipo OBJECT porque requer seleção de sub-campo (o que não deveria ocorrer quando `gql_selection` está disponível), o executor tenta novamente uma vez com esses campos removidos para que colunas escalares ainda sejam retornadas. Isso se aplica a tabelas lidas a partir de um campo raiz. Uma tabela de conexão não segue esse caminho.

**Leituras paginadas.** Uma tabela de conexão é lida por cursor. Cada página pede `first: N, after: $pageCursor` com `pageInfo { hasNextPage endCursor }`, e a leitura segue `endCursor` até que o remoto informe que não há próxima página.

| Configuração | Padrão | Efeito |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Linhas por página. |
| `graphql_remote.max_rows` | `10000` | O máximo de linhas que uma leitura de uma tabela de conexão pega. Uma leitura que o atinge para e registra um aviso. |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Duas respostas fazem o executor tentar novamente:

- **Página pesada demais.** Quando o remoto responde 502 ou 504, a mesma página é pedida de novo com metade do tamanho, até uma linha.
- **Limite de taxa com tempo de espera.** Quando o remoto responde 403 ou 429 com um `Retry-After` de 120 segundos ou menos, o executor espera esse tempo e envia a requisição novamente, até três tentativas. Uma recusa sem `Retry-After`, ou que peça uma espera maior, é levantada como erro. Isso se aplica a toda leitura, de conexão ou não.

Qualquer outro erro na resposta faz a leitura falhar, a menos que o tipo de fonte declare o contrário (veja GitHub abaixo). Uma conexão cujo pai voltou nulo não tem linhas.

---

### GitHub

GitHub é um tipo de fonte comum. Sua API é GraphQL, então suas tabelas se comportam como descrito acima, incluindo tabelas de conexão como `gh__repository_issues`.

**Adicionar a fonte.**

1. Abra Sources e adicione uma fonte do tipo **GitHub**.
2. Informe um token de acesso do GitHub. Opcionalmente informe um namespace, o prefixo dos nomes de tabela; o padrão é `gh`.
3. Salve. O Provisa verifica o token no GitHub. Um token que o GitHub rejeita faz a adição falhar com a mensagem do GitHub.

Adicionar a fonte não registra nenhuma tabela.

**Registrar tabelas.** Abra Tables, depois Register Table. Escolha a fonte GitHub, escolha o esquema `graphql` e então escolha as tabelas desejadas. Toda tabela que o GitHub oferece é listada; o registro é a sua escolha do que expor.

**Escopos (scopes) do token.** Quando você registra uma tabela, o Provisa a verifica uma vez no GitHub com o seu token.

- Um campo que os escopos do token não cobrem fica de fora da tabela. O resultado nomeia cada campo deixado de fora: `Left out, because the source's credential may not read them: projectsV2`.
- Uma tabela que o token não consegue ler de forma alguma é recusada, com o motivo do GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`.

**Linhas que o token não pode ver.** O GitHub responde `FORBIDDEN` para um campo que o token não pode ver em uma linha específica, como os colaboradores de um repositório sem acesso de escrita, e `NOT_ORG_OWNED_REPO` para um campo que existe apenas em repositórios pertencentes a uma organização. Esse campo é nulo nessa linha, o restante da leitura continua e o Provisa registra um aviso. Um erro contra a própria tabela faz a leitura falhar.

**Páginas pesadas.** Quando o GitHub responde `RESOURCE_LIMITS_EXCEEDED` porque uma página custa caro demais para calcular, a página é pedida de novo com metade do tamanho.

**Objetos aninhados.** As tabelas do GitHub usam uma profundidade de aninhamento própria, 0 (`max_object_depth=0` para esse tipo de fonte), em vez de `graphql_remote.max_object_depth`. Uma coluna de objeto aninhado é selecionada só com seus próprios campos escalares, e os objetos dentro dela aparecem como `__typename`.

**Armazenamento do token.** O token vai para o cofre de segredos e a linha da fonte guarda uma referência, de modo que uma reinicialização lê a fonte novamente sem reinformar o token.

**Como funciona (operadores).** O esquema do GitHub é distribuído com o Provisa, então adicionar a fonte não faz nenhuma chamada de introspecção e um esquema grande não custa nada no registro. As tabelas são mapeadas a partir dele uma a uma conforme você as registra. O endpoint de refresh recusa este tipo de fonte; um novo esquema do GitHub chega com uma versão do Provisa.

---

### GitLab

GitLab é um tipo de fonte comum e é adicionado e registrado da mesma forma que o GitHub: adicione uma fonte do tipo **GitLab** com um token de acesso e depois registre as tabelas desejadas do esquema `graphql`. O prefixo padrão dos nomes de tabela é `gl`. A fonte alcança `gitlab.com`.

**Escolher colunas.** O GitLab atribui um custo a cada consulta e recusa a que custa demais: 200 pontos para um chamador anônimo, 250 com token. Uma tabela larga com todas as colunas selecionadas passa desse custo, então registre uma tabela do GitLab com as colunas que você deseja.

- Quando você registra uma tabela, o Provisa pergunta uma vez ao GitLab se ele atenderá a seleção no tamanho de página que as leituras usam. Se o GitLab responder que a consulta é complexa ou grande demais, a tabela não é registrada e o resultado traz a mensagem do GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- O que uma coluna custa depende do seu tipo. Um valor simples custa cerca de um ponto; uma coluna de objeto aninhado custa muitas vezes isso. Descartar colunas de objeto aninhado libera mais.
- O tamanho da página faz parte do custo. É `graphql_remote.max_list_items`.

**Verificação do token.** O GitLab responde a um token não reconhecido com um resultado vazio em vez de um erro. O Provisa trata isso como um token recusado e não adiciona a fonte.

---

### Esquema remoto gRPC {: #grpc-remote-schema }

**Como adicionar a fonte.** Faça POST para `/admin/grpc-remote/register` com o endereço do servidor, um caminho ou URL para um arquivo `.proto`, e configuração TLS opcional. Adicionar a fonte não registra nenhuma tabela.

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

O Provisa busca o proto, o analisa com um parser de texto puro (sem dependências proto externas no momento da análise), compila stubs Python via `grpc_tools.protoc`, e abre um `grpc.aio.Channel` persistente.

Arquivos proto também podem ser caminhos locais. Caminhos de importação para tipos bem conhecidos (`google/protobuf/timestamp.proto`) são armazenados no momento do registro e reutilizados na atualização.

**O que a fonte oferece.** Todo método `rpc` no proto é classificado como query ou mutation usando três sinais em ordem de prioridade:

1. **`method_overrides`** no payload de registro — `{"MethodName": "query"}` ou `{"MethodName": "mutation"}` sobrepõe todo o resto.
2. **`server_streaming: true`** — o servidor envia um stream de mensagens; sempre uma tabela virtual (a menos que a saída seja um escalar).
3. **A mensagem de saída tem um campo repetido do tipo mensagem** — ex.: `ListOrdersResponse { repeated Order items; }` é tratado como um envoltório de lista e se torna uma tabela virtual. Campos escalares repetidos (ex.: `repeated string tags`) não disparam isso, porque são propriedades de array em uma única entidade, não fontes de linha.

Métodos que não correspondem a nenhum desses sinais (RPC unário retornando uma única mensagem de entidade, ou qualquer saída escalar) se tornam funções rastreadas.

**Registrar tabelas.** Cada método de query é oferecido como uma tabela, nomeada `{namespace}__{Service}__{Method}`, sob o esquema de seletor `grpc_remote`. Registre as desejadas com o seletor Register Table (`availableTables`, `availableColumns`, `registerTable`, como para fontes GraphQL), escolhendo as colunas de resposta. Os campos da requisição viram colunas de filtro nativo `_nf_*`, e essas são sempre incluídas.

Métodos de mutação são commands oferecidos, contados em `available_mutations`; adicionar a fonte não registra nenhum. Uma mutação gRPC é registrada na página Comandos, escolhendo a fonte e depois o método, chamado `Service.Method`; o tipo do command é `source_operation`. Veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation).

**Nomeação de tabela.** O nome padrão é `{namespace}__{ServiceName}__{MethodName}`. Sem um namespace, os nomes de serviço e método são unidos diretamente. Qualquer tabela registrada pode receber um `alias`; quando definido, o alias é o nome usado em todo lugar (consultas, SDL, relacionamentos). O nome auto-gerado é a chave de registro e nunca muda.

**Mapeamento de tipo.** Tipos escalares proto mapeiam para tipos SQL como segue.

| Tipo Proto | Tipo SQL |
| --- | --- |
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Mensagem aninhada | `jsonb` |
| Enum | `text` |

**Relacionamentos no registro.** `relationships` funciona como no adaptador GQL: declara caminhos de join FK/PK, salvos como relacionamentos declarados manualmente (sem a flag `remote_managed`). Uma atualização os deixa como estão.

**Métodos de query.** Campos da mensagem de saída se tornam colunas de tabela. Campos da mensagem de entrada se tornam tanto argumentos GraphQL passados à chamada remota *quanto* são registrados como colunas prefixadas com `_nf_` com `native_filter_type: "grpc_input"`, o mesmo mecanismo que GQL e OpenAPI usam para injeção de filtro nativo.

**Sub-campos de mensagem aninhada.** Para métodos de query, campos do tipo mensagem não repetidos na profundidade 0 (colunas de saída diretas) têm seus sub-campos resolvidos um nível mais profundo e armazenados como `object_fields` no `ColumnDef`. Este metadado é usado para extração de sub-campo `jsonb` em SQL e para documentação de esquema. Campos aninhados além da profundidade 1 não são expandidos recursivamente.

Métodos server-streaming coletam todas as mensagens transmitidas em uma lista antes de retornar linhas.

**Métodos de mutação.** Um método de mutação registrado é um command cujos argumentos são os campos da mensagem de entrada, cada um tipado como `json` e repassado sem alteração. A resposta do serviço remoto volta como linhas; uma chamada recusada é um 422, `functions.remote_refused`. Veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation).

**Gerenciamento de canal.** Um `grpc.aio.Channel` por fonte registrada é armazenado no estado da app e reutilizado através das requisições. O canal antigo é fechado antes que um novo abra na atualização.

**Atualização (refresh).** Faça POST para `/admin/grpc-remote/refresh/{source_id}`. Recarrega o proto do caminho armazenado, recompila os stubs e atualiza, conforme o proto, as tabelas já registradas, com as colunas com que cada uma foi registrada. Não registra nenhuma tabela nova; um método de query adicionado ao proto continua oferecido. Como alternativa, faça PUT para `/admin/grpc-remote/{source_id}/proto` com um novo `proto_text` para atualizar o proto inline.

**Limitações.**

- A extração de objeto de sub-campo é de um nível de profundidade. Campos de mensagem aninhados além da profundidade 1 não são expandidos recursivamente.

---

### OpenAPI / REST

**Como adicionar a fonte.** Faça POST para `/admin/openapi/register` com um ID de fonte e uma spec, carregada de um arquivo local ou de uma URL. A spec é analisada e mantida com a fonte; nenhuma tabela nem command é registrado. A resposta informa `tables: 0` e `mutations: 0`, com os contadores oferecidos em `available_tables` e `available_mutations`.

**Registrar tabelas.** Registre cada operação GET desejada pelo seletor Register Table (`availableTables`, `availableColumns`, `registerTable`), escolhendo as colunas. Registre cada operação não-GET individualmente como command na página Comandos, listadas por `availableFunctions`; veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation). `PUT /admin/openapi/spec/{source_id}` armazena uma spec editada à mão, não registra nada e retorna `available_tables` e `available_mutations`.

**Payload de registro.** O endpoint `/admin/openapi/register` aceita dois campos adicionais ao lado de `source_id`, `spec_path`, etc.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**O que a fonte oferece.** Toda operação GET da especificação é oferecida como tabela, a menos que seu esquema de resposta seja um tipo escalar (`string`, `number`, `boolean`, `integer`); um GET que devolve um escalar é uma função com uma única coluna `value`. Toda outra operação (POST, PUT, PATCH, DELETE) é oferecida como command, com o nome do seu `operationId`. Depois de registrada, ela recebe os parâmetros de caminho da operação e um argumento `body` para o corpo da requisição, cada um do tipo `json`, e qualquer outro argumento vai na query string. Veja [Operação de escrita de uma fonte remota](commands.md#a-remote-sources-write-operation).

Prioridade de classificação: `operation_overrides` (payload) sobrepõe `x-provisa-kind` (extensão de spec) sobrepõe a heurística GET. `operation_overrides` é o caminho de override recomendado; `x-provisa-kind` é para quando a própria spec deve carregar a classificação.

**Relacionamentos no registro.** `relationships` funciona como nos outros adaptadores: salvos como relacionamentos declarados manualmente e mantidos na atualização.

**Nomeação de tabela.** Tabelas usam o `operationId` da operação. Se nenhum `operationId` for definido, o Provisa faz slugify de `{method}_{path}`. Um alias é derivado removendo o segmento de verbo inicial e singularizando o substantivo (`findPetsByStatus` → `pet_by_status`).

**Mapeamento de tipo.** Tipos JSON Schema mapeiam para tipos Provisa como segue.

| Tipo JSON Schema | Tipo Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parâmetros como colunas de filtro nativo.** Parâmetros de caminho e de consulta que ainda não são campos da resposta viram colunas com `native_filter_type` igual a `path_param` ou `query_param`, com o prefixo `_nf_`. Quando um parâmetro tem o mesmo nome de um campo da resposta, seus metadados são mesclados na entrada dessa coluna, sem criar duplicata.

**Resolução de esquema de resposta.** O mapper verifica `responses.200`, depois `responses.2xx`, depois `responses.default`. Respostas do tipo array são desempacotadas para o esquema do item. Referências `$ref` são resolvidas um nível de profundidade.

**Sub-campos de objeto.** Propriedades de resposta com `type: object` e suas próprias `properties` são armazenadas como `object_fields` na coluna. Esses sub-campos são visíveis na SDL e usados para extração `jsonb` em consultas.

**Cache de resposta.** Resultados de operação GET são cacheados no PostgreSQL por `pg_cache.py`. Cada combinação de parâmetros de requisição obtém seu próprio grupo `_params_hash`. Linhas para um dado hash são substituídas quando o TTL expira. Endpoints com parâmetro de path (`/pets/{id}`) pulam a busca em massa inicial. A tabela de cache é criada vazia para introspecção de esquema, depois populada por PK conforme as requisições chegam.

**Atualização.** Faça POST para `/admin/openapi/refresh/{source_id}`. Reanalisa a spec por meio de `_load_and_register`, que não registra nada: não adiciona nenhuma tabela nem coluna. Regras de governança existentes são preservadas. Uma tabela registrada mantém suas colunas; ela é lida pela operação da spec atualizada.

**Limitações.**

- A extração de objeto de sub-campo é de um nível de profundidade. Propriedades aninhadas dentro de `object_fields` não são expandidas recursivamente.
- Parâmetros de header e cookie são ignorados; somente parâmetros `path` e `query` são registrados.
- A resolução de `$ref` em nível de spec é de um nível de profundidade para esquemas de propriedade; referências de componente profundamente aninhadas podem não resolver.

---

## Impacto de registrar uma tabela remota {: #impact-of-registering-a-remote-table }

Uma tabela registrada a partir de qualquer fonte de esquema remoto é uma tabela Provisa de primeira classe. Nada nela é tratado de forma diferente de uma tabela relacional conectada localmente em tempo de execução.

**Interfaces de consulta.** A tabela é imediatamente consultável via GraphQL, SQL (pgwire ou direto), Cypher (GQL), JSON:API, e Arrow Flight. A geração de esquema sintetiza `ColumnMetadata` para tabelas remotas já que elas não têm catálogo; o mapeamento de tipo é aplicado no momento da construção do esquema.

**Modelo de segurança.** Todas as cinco camadas de governança se aplicam:

1. Controle de acesso a domínio — o `domain_id` da tabela condiciona quais funções conseguem vê-la.
2. Segurança em nível de linha (RLS) — filtros de linha configurados na tabela são injetados em toda consulta, independentemente da interface.
3. Visibilidade de coluna — a lista `visible_to` em cada coluna controla a exposição de campo por função.
4. Mascaramento de coluna — regras de mascaramento se aplicam no Estágio 2 do pipeline de governança.
5. Guard de predicado — colunas mascaradas são rejeitadas de cláusulas WHERE e HAVING.

Consultas ad hoc em tabelas remotas são permitidas só com os direitos do usuário. O acesso segue em todo lugar o mesmo modelo baseado em direitos (direitos de tabela e de coluna mais relacionamentos aprovados), sem modo de governança por tabela.

**Governança de relacionamentos (V002).** Uma condição JOIN sobre tabelas remotas, numa consulta SQL ou Cypher, precisa corresponder a um relacionamento registrado e aprovado. Consultas GraphQL pulam a verificação V002, porque os relacionamentos definidos no SDL são aprovados por concepção. Veja [docs/security.md](security.md#relationship-governance-v002).

**Colunas do tipo OBJECT.** Quando uma coluna mapeia para um OBJECT GQL inline não governado ou tipo de objeto OpenAPI, seu tipo Provisa é `jsonb`. A coluna armazena o blob JSON aninhado completo. Quando sub-campos são declarados (`gql_object_fields` ou `object_fields`), o mapa `gql_object_columns` é populado no momento da construção do esquema. O gerador SQL usa esse mapa para emitir expressões de extração `->>` para sub-campos quando uma consulta os seleciona.

**Args obrigatórios como parâmetros de filtro nativo.** Campos de consulta raiz com args non-null, sem padrão injetam colunas adicionais na tabela registrada. Essas colunas carregam `native_filter_type: query_param`. O tradutor Cypher reescreve `WHERE n.id = $val` para `WHERE n._nf_id = $val`, e o executor GraphQL as recolhe como variáveis para passar ao endpoint remoto.

---

## Impacto de criar um relacionamento de cobertura {: #impact-of-creating-a-covering-relationship }

Quando um steward registra um relacionamento entre duas tabelas remotas (ou entre uma tabela remota e uma tabela local), o relacionamento se torna o caminho de join usado no momento da consulta.

**Como o join prevalece.** Na compilação da consulta, o Provisa resolve o caminho de join através do relacionamento registrado. `source_column` e `target_column` no relacionamento se tornam a condição de join no SQL gerado. O join substitui qualquer chamada remota por tabela que de outra forma seria necessária para o tipo conectado.

**O blob bruto nunca é exposto em SQL.** Não dá para selecionar a coluna `breed` de `petstore__pets` como valor jsonb bruto numa consulta SQL. Assim que um relacionamento é registrado entre `petstore__pets` e `petstore__breeds`, as consultas SQL percorrem o join: `SELECT breed.name FROM petstore__pets` se resolve pelo join de FK. Quando não há relacionamento registrado mas a coluna tem subcampos declarados (`gql_object_fields`), as referências SQL a subcampos são reescritas como extração `->>` do blob guardado. Esse caminho só existe para tipos inline não governados; campos cujo destino é governado ficam totalmente fora do SDL e não têm blob de onde extrair. O próprio blob bruto nunca é devolvido como valor de coluna puro.

No SDL do GraphQL, um campo OBJECT inline não governado tem como tipo o tipo de objeto aninhado. Se na execução quem o serve é um join ou uma extração do blob, a forma do SDL não muda. Assim que o tipo filho é registrado como tabela própria, e portanto governado, as cinco camadas de governança se aplicam a ele de forma independente: as próprias regras de RLS, visibilidade de colunas, regras de mascaramento, guardas de predicado e controle de acesso por domínio. A extração do blob passa por cima de tudo isso, porque os dados filhos chegam embutidos na linha pai e só as regras da tabela pai os governam. Para governar o tipo filho em detalhe, registre-o como tabela e crie um relacionamento.

**`graphql_alias` no relacionamento.** O campo `graphql_alias` nomeia o campo SDL que o relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e da cardinalidade do relacionamento via `rel_field_name(target.field_name, cardinality)`.

**V002 no caminho de join.** Consultas SQL e Cypher que percorrem o relacionamento estão sujeitas à governança de relacionamento V002. O relacionamento deve ser registrado e aprovado para que o join seja permitido. A travessia GraphQL via campo de relacionamento SDL é sempre pré-aprovada.

**Flag remote-managed.** Relacionamentos auto-detectados durante o registro remoto GraphQL são armazenados com `remote_managed: True`. Este é um marcador de metadado; não altera o comportamento de governança.

---

## Comportamento somente-type-def {: #type-def-only-behavior }

Nem todo tipo em um esquema remoto precisa ser uma tabela consultável.

Quando `root_table_ids` é definido em um `SchemaInput`, tabelas cujos IDs estão ausentes desse conjunto são excluídas dos campos de consulta raiz na SDL gerada. Elas permanecem presentes como tipos GraphQL e podem ser alcançadas via campos de relacionamento em tabelas que têm entradas raiz.

O mesmo mecanismo vale quando o esquema é construído filtrado por domínio. Tabelas de domínios a que o papel não tem acesso são só definições de tipo: a definição de tipo delas existe no SDL para percorrer relacionamentos, mas nenhum campo de consulta raiz é gerado para elas.

Uma tabela somente-type-def:

- Não tem campo de consulta raiz, então os clientes não conseguem consultá-la diretamente pelo nome.
- É alcançável via campos de relacionamento em tabelas que têm entradas raiz.
- Ainda aparece na introspecção de esquema como um tipo nomeado.
- Ainda tem todas as regras de governança aplicadas quando dados são acessados através de um relacionamento.

Uma tabela, incluindo a definição de tipo, só sai totalmente do esquema quando o registro dela é apagado. Tornar uma tabela apenas definição de tipo (tirando o ID dela de `root_table_ids`, ou pelo filtro de acesso a domínios) não remove o tipo.

Assim os stewards podem expor grafos de objetos navegáveis em que alguns tipos só são alcançados por travessia e não podem ser consultados sozinhos.
