# Esquemas Remotos (Remote Schemas)

Uma fonte de esquema remoto conecta uma API externa — GraphQL (incluindo GitHub), gRPC, ou REST (OpenAPI) — ao modelo do Provisa. Adicionar uma fonte não registra nenhuma tabela. A fonte oferece tabelas, e um steward registra cada tabela desejada pelo seletor «Register Table»; esse registro é a etapa de curadoria. (REQ-308, REQ-316, REQ-322) Uma tabela registrada é uma tabela Provisa de primeira classe. (REQ-308, REQ-316, REQ-325) Toda regra de governança, interface de consulta, e camada de segurança se aplica automaticamente. (REQ-310, REQ-319, REQ-328) O serviço remoto nunca vê as regras de governança do Provisa. (REQ-310, REQ-319, REQ-328)

---

## Três tipos de fonte

### Esquema remoto GraphQL (REQ-307–313)

**Como adicionar a fonte.** Faça POST para `/admin/sources/graphql-remote` com a URL do endpoint, um namespace, e auth opcional. O Provisa dispara uma consulta de introspecção `__schema` padrão contra o endpoint remoto para confirmar o endpoint e a credencial. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

Adicionar a fonte não registra nenhuma tabela nem nenhum command. Todo tipo de fonte remota responde à adição e à atualização com os mesmos contadores: `tables` (tabelas registradas ou atualizadas; 0 na adição), `available_tables` (tabelas oferecidas), `mutations` (sempre 0) e `available_mutations` (commands oferecidos). [tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**Registrar tabelas.** Abra Tables, depois Register Table, escolha a fonte e o esquema `graphql`, e selecione as tabelas e colunas desejadas. Pela API GraphQL de administração: `availableTables(sourceId, schemaName)` lista as tabelas oferecidas, `availableColumns` lista as colunas de uma tabela, e `registerTable(input: TableInput)` registra uma com as colunas escolhidas. Uma tabela registrada passa então a ser governada. (REQ-308) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`] [tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

Como uma tabela registrada é lida (campo raiz, caminho das linhas, argumentos obrigatórios, argumentos de paginação) fica armazenado em `sources.mapping["tables"]`, de modo que um processo reiniciado a lê sem pedir ao remoto o seu esquema. [tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

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

Opções de auth: `none`, `bearer` (header Authorization), `basic` (Base64 username:password). (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Overrides de campo.** `field_overrides` é um mapa `{fieldName: "query" | "mutation"}` aplicado após a introspecção. Tem prioridade sobre a classificação estrutural. Somente campos do tipo query podem ser reclassificados como mutations; campos do tipo mutation não têm caminho de override no GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relacionamentos no momento do registro.** `relationships` declara caminhos de join FK/PK entre tabelas no momento do registro. Estes são armazenados como relacionamentos declarados manualmente (sem flag `remote_managed`). Na atualização, relacionamentos auto-detectados (aqueles com `remote_managed: True`) são reexecutados e podem mudar; relacionamentos declarados manualmente não são tocados. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**O que a fonte oferece.** Todo campo do tipo `Query` remoto que retorna um objeto ou uma lista de objetos é oferecido como tabela, e também cada conexão Relay sob um campo de objeto único (veja abaixo). Registrar uma tabela oferecida a torna uma tabela. Cada campo do tipo `Mutation` remoto é um command oferecido, contado em `available_mutations`; adicionar a fonte não registra nenhum. Registre os que quiser como commands; veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). (REQ-308, REQ-1924) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**Nomeação de tabela.** Tabelas são nomeadas `{namespace}__{field_name}`. Com namespace `petstore` e um campo de consulta `pets`: o nome da tabela é `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Conexões Relay.** Muitas APIs retornam listas como conexões Relay: um objeto com `nodes` (ou `edges { node }`) ao lado de `pageInfo`. O Provisa mapeia uma conexão para uma tabela dos seus nós e a lê página por página. (REQ-308, REQ-309) [tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- Um campo raiz que retorna uma conexão (`securityAdvisories`) vira uma tabela dos seus nós.
- Uma conexão no objeto único que um campo raiz retorna vira sua própria tabela. A tabela recebe os argumentos obrigatórios do campo raiz. Com `repository(owner, name)` e uma conexão `issues` em `Repository`, a tabela é `repositoryIssues`, com nome SQL `gh__repository_issues` sob o namespace `gh`. Filtre-a pelas colunas `_nf_owner` e `_nf_name`: `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Uma conexão nunca é uma coluna. Caso contrário, uma linha carregaria uma leitura que o remoto calcula por linha, para cada conexão que o seu tipo tem.
- Uma conexão só é tabela se o seu campo aceita `first` e `after`, de modo que possa ser lida página por página. Uma que precisa de um argumento próprio não é tabela. Também não é uma conexão de uma união, nem qualquer conexão sob um campo raiz que retorna uma lista.

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**Mapeamento de tipo (REQ-308).** Campos escalares mapeiam diretamente para tipos Provisa. Campos OBJECT se dividem em dois casos dependendo se o tipo alvo é governado (veja "Tabelas governadas" abaixo). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

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

**Tabelas governadas.** Um tipo GQL é governado quando aparece como um campo raiz `Query` no esquema remoto. `_collect_queryable_types` coleta estes durante o registro, preferindo campos sem argumento obrigatório para que possam ser buscados em massa como alvos de join. [tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

Quando uma coluna do tipo OBJECT em uma tabela governada aponta para outro tipo governado, essa coluna está sujeita a três regras simultaneamente [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Excluída da busca GQL** — o campo não é solicitado ao buscar linhas da tabela pai.
2. **Excluída da SDL** — o campo não aparece no tipo pai no esquema gerado.
3. **Acessível somente via um relacionamento declarado** — um steward deve registrar um JOIN entre as duas tabelas governadas materializadas. Sem um, o campo simplesmente está ausente; não há fallback de blob.

Tipos OBJECT que NÃO são alcançáveis como campos Query raiz (tipos inline como `ContactInfo` ou `Address`) seguem regras diferentes: são buscados como colunas blob `jsonb` e aparecem na SDL como campos de objeto aninhado. Sub-campos são acessíveis via extração `-->>` em SQL.

**Campos que precisam de um argumento não são colunas.** Um campo com um argumento obrigatório não pode ser selecionado diretamente, então fica de fora das colunas da tabela e das seleções aninhadas. [tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**Argumentos obrigatórios.** Quando um campo de consulta raiz tem argumentos non-null sem valor padrão, esses se tornam colunas `native_filter_type: query_param` na tabela (prefixadas com `_nf_` no momento da injeção). O executor as passa como variáveis GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relacionamentos detectados automaticamente.** O Provisa examina as colunas do tipo OBJECT de cada tabela registrada. Quando o tipo GQL referenciado também é uma tabela registrada na mesma fonte, e a coluna em que o relacionamento se apoia está entre as colunas registradas, o relacionamento é armazenado. Uma tabela ainda não registrada não recebe nenhum. [tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`] Relacionamentos many-to-one inferem as colunas de origem e destino a partir de convenções de nomenclatura (`breedName` no tipo de origem → `name` no tipo de destino `Breed`). Campos one-to-many (LIST) emitem relacionamentos com referências de coluna vazias — a FK fica no lado do destino. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutações.** Um campo de mutação é registrado, um a um, como um command do tipo `source_operation`. Seus argumentos são cada um tipado como `json` e repassados ao serviço remoto como variáveis tipadas; a resposta é o JSON que o serviço remoto retorna, sem `return_schema`. Veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). (REQ-1924) [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**Atualização (refresh).** Faça POST para `/admin/sources/graphql-remote/{id}/refresh`. Reintrospecciona o esquema remoto e atualiza, conforme ele, as tabelas já registradas. Não adiciona nenhuma tabela nem coluna: uma tabela ou coluna que o esquema ganhou continua oferecida, e uma coluna que o esquema perdeu é removida. Regras de governança existentes (RLS, mascaramento) são preservadas. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**Limitações.**

- Campos de consulta raiz escalares e ENUM (tipo de retorno não é OBJECT) se tornam funções rastreadas, não tabelas virtuais. Seu `return_schema` é uma única coluna `value` do tipo escalar mapeado. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- O aninhamento de objeto é resolvido no momento do registro até `graphql_remote.max_object_depth` (padrão: 5). Tanto a seleção da busca remota quanto os metadados dos sub-campos são construídos até essa profundidade; campos além do limite não são buscados e não estão disponíveis para extração em SQL. Um tipo é visitado uma só vez ao longo de qualquer caminho: um campo cujo tipo já está no caminho descendente é deixado de fora, de modo que um esquema cujos tipos se referem uns aos outros é percorrido uma vez por tipo, não uma vez por nível de profundidade. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- Campos OBJECT aninhados do tipo LIST (ex.: `breed.awards: [Award]`) são incluídos na seleção de busca até `graphql_remote.max_list_depth` níveis de aninhamento (padrão: 2). Dentro desse limite, a lista é buscada como um array `jsonb` na coluna pai. Quando o campo de lista declara um argumento `first` (Relay, PostGraphile, pg_graphql) ou um argumento `limit` (Hasura), a seleção o passa como `first: N` ou `limit: N`, em que N é `graphql_remote.max_list_items` (padrão: 100). Um campo de lista que não declara nenhum dos dois não recebe argumento, porque um remoto rejeita um argumento que o campo não declara. Além de `max_list_depth`, o campo LIST é excluído por completo para evitar expansão ilimitada de dados. Em SQL, o array é acessado via `json_array_elements(column_name)` ou extração por índice com `->>`. Se o tipo de item da lista tiver sua própria consulta raiz, registre-o como uma tabela separada e crie um relacionamento — o caminho de join é mais eficiente e contorna o blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- Para consultas SQL, colunas do tipo OBJECT não governadas são buscadas por completo do remoto (todos os sub-campos até a profundidade configurada) e cacheadas como `jsonb`. O acesso a sub-campo em SQL é tratado via extração `->>` contra o blob; a requisição remota não é reduzida somente aos campos que a consulta SQL seleciona. Quando o tipo de item da LIST não tem consulta raiz e a representação em blob é insuficiente, escreva a consulta em SDL GraphQL diretamente — o Provisa reproduz fielmente a seleção de campo GQL, para que o remoto veja exatamente os campos solicitados. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Se o servidor remoto rejeitar um campo do tipo OBJECT porque requer seleção de sub-campo (o que não deveria ocorrer quando `gql_selection` está disponível), o executor tenta novamente uma vez com esses campos removidos para que colunas escalares ainda sejam retornadas. Isso se aplica a tabelas lidas a partir de um campo raiz. Uma tabela de conexão não segue esse caminho. [tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**Leituras paginadas.** Uma tabela de conexão é lida por cursor. Cada página pede `first: N, after: $pageCursor` com `pageInfo { hasNextPage endCursor }`, e a leitura segue `endCursor` até que o remoto informe que não há próxima página. (REQ-309) [tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| Configuração | Padrão | Efeito |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Linhas por página. [tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | O máximo de linhas que uma leitura de uma tabela de conexão pega. Uma leitura que o atinge para e registra um aviso. [tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Duas respostas fazem o executor tentar novamente:

- **Página pesada demais.** Quando o remoto responde 502 ou 504, a mesma página é pedida de novo com metade do tamanho, até uma linha. [tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **Limite de taxa com tempo de espera.** Quando o remoto responde 403 ou 429 com um `Retry-After` de 120 segundos ou menos, o executor espera esse tempo e envia a requisição novamente, até três tentativas. Uma recusa sem `Retry-After`, ou que peça uma espera maior, é levantada como erro. Isso se aplica a toda leitura, de conexão ou não. [tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

Qualquer outro erro na resposta faz a leitura falhar, a menos que o tipo de fonte declare o contrário (veja GitHub abaixo). Uma conexão cujo pai voltou nulo não tem linhas. [tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub (REQ-1923)

GitHub é um tipo de fonte comum. Sua API é GraphQL, então suas tabelas se comportam como descrito acima, incluindo tabelas de conexão como `gh__repository_issues`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**Adicionar a fonte.**

1. Abra Sources e adicione uma fonte do tipo **GitHub**.
2. Informe um token de acesso do GitHub. Opcionalmente informe um namespace, o prefixo dos nomes de tabela; o padrão é `gh`.
3. Salve. O Provisa verifica o token no GitHub. Um token que o GitHub rejeita faz a adição falhar com a mensagem do GitHub.

Adicionar a fonte não registra nenhuma tabela. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**Registrar tabelas.** Abra Tables, depois Register Table. Escolha a fonte GitHub, escolha o esquema `graphql` e então escolha as tabelas desejadas. Toda tabela que o GitHub oferece é listada; o registro é a sua escolha do que expor. [tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`] [inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**Escopos (scopes) do token.** Quando você registra uma tabela, o Provisa a verifica uma vez no GitHub com o seu token.

- Um campo que os escopos do token não cobrem fica de fora da tabela. O resultado nomeia cada campo deixado de fora: `Left out, because the source's credential may not read them: projectsV2`. [tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- Uma tabela que o token não consegue ler de forma alguma é recusada, com o motivo do GitHub: `GitHub does not let this source's credential read gh__repository_issues: ...`. [tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**Linhas que o token não pode ver.** O GitHub responde `FORBIDDEN` para um campo que o token não pode ver em uma linha específica, como os colaboradores de um repositório sem acesso de escrita, e `NOT_ORG_OWNED_REPO` para um campo que existe apenas em repositórios pertencentes a uma organização. Esse campo é nulo nessa linha, o restante da leitura continua e o Provisa registra um aviso. Um erro contra a própria tabela faz a leitura falhar. [tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**Páginas pesadas.** Quando o GitHub responde `RESOURCE_LIMITS_EXCEEDED` porque uma página custa caro demais para calcular, a página é pedida de novo com metade do tamanho. [tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**Objetos aninhados.** Tabelas do GitHub usam sua própria profundidade de aninhamento 0 (`max_object_depth=0` para este tipo de fonte), não `graphql_remote.max_object_depth`. Uma coluna de objeto aninhado é selecionada apenas com seus próprios campos escalares; objetos dentro dela aparecem como `__typename`. [tool-verified: `brands.py`]

**Armazenamento do token.** O token vai para o cofre de segredos e a linha da fonte guarda uma referência, de modo que uma reinicialização lê a fonte novamente sem reinformar o token. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**Como funciona (operadores).** O esquema do GitHub é distribuído com o Provisa, então adicionar a fonte não faz nenhuma chamada de introspecção e um esquema grande não custa nada no registro. As tabelas são mapeadas a partir dele uma a uma conforme você as registra. O endpoint de refresh recusa este tipo de fonte; um novo esquema do GitHub chega com uma versão do Provisa. [tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note] [tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab (REQ-1923)

GitLab é um tipo de fonte comum e é adicionado e registrado da mesma forma que o GitHub: adicione uma fonte do tipo **GitLab** com um token de acesso e depois registre as tabelas desejadas do esquema `graphql`. O prefixo padrão dos nomes de tabela é `gl`. A fonte alcança `gitlab.com`. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**Escolher colunas.** O GitLab atribui um custo a cada consulta e recusa a que custa demais: 200 pontos para um chamador anônimo, 250 com token. Uma tabela larga com todas as colunas selecionadas passa desse custo, então registre uma tabela do GitLab com as colunas que você deseja. [tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- Quando você registra uma tabela, o Provisa pergunta uma vez ao GitLab se ele atenderá a seleção no tamanho de página que as leituras usam. Se o GitLab responder que a consulta é complexa ou grande demais, a tabela não é registrada e o resultado traz a mensagem do GitLab: `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.` [tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- O que uma coluna custa depende do seu tipo. Um valor simples custa cerca de um ponto; uma coluna de objeto aninhado custa muitas vezes isso. Descartar colunas de objeto aninhado libera mais. [tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- O tamanho da página faz parte do custo. É `graphql_remote.max_list_items`. [tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**Verificação do token.** O GitLab responde a um token não reconhecido com um resultado vazio, não com um erro. O Provisa trata isso como um token rejeitado e não adiciona a fonte. [tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### Esquema remoto gRPC (REQ-322–329)

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

O Provisa busca o proto, o analisa com um parser de texto puro (sem dependências proto externas no momento da análise), compila stubs Python via `grpc_tools.protoc`, e abre um `grpc.aio.Channel` persistente. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Arquivos proto também podem ser caminhos locais. Caminhos de importação para tipos bem conhecidos (`google/protobuf/timestamp.proto`) são armazenados no momento do registro e reutilizados na atualização. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**O que a fonte oferece.** Todo método `rpc` no proto é classificado como query ou mutation usando três sinais em ordem de prioridade: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** no payload de registro — `{"MethodName": "query"}` ou `{"MethodName": "mutation"}` sobrepõe todo o resto.
2. **`server_streaming: true`** — o servidor envia um stream de mensagens; sempre uma tabela virtual (a menos que a saída seja um escalar).
3. **A mensagem de saída tem um campo repetido do tipo mensagem** — ex.: `ListOrdersResponse { repeated Order items; }` é tratado como um envoltório de lista e se torna uma tabela virtual. Campos escalares repetidos (ex.: `repeated string tags`) não disparam isso — são propriedades de array em uma única entidade, não fontes de linha.

Métodos que não correspondem a nenhum desses sinais (RPC unário retornando uma única mensagem de entidade, ou qualquer saída escalar) se tornam funções rastreadas.

**Registrar tabelas.** Cada método de query é oferecido como uma tabela, nomeada `{namespace}__{Service}__{Method}`, sob o esquema de seletor `grpc_remote`. Registre as desejadas com o seletor Register Table (`availableTables`, `availableColumns`, `registerTable`, como para fontes GraphQL), escolhendo as colunas de resposta. Os campos da requisição viram colunas de filtro nativo `_nf_*`, e essas são sempre incluídas. (REQ-322) [tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Métodos de mutação são commands oferecidos, contados em `available_mutations`; adicionar a fonte não registra nenhum. Uma mutação gRPC é registrada na página Comandos, escolhendo a fonte e depois o método, chamado `Service.Method`; o tipo do command é `source_operation`. Veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**Nomeação de tabela.** O nome padrão é `{namespace}__{ServiceName}__{MethodName}`. Sem um namespace, os nomes de serviço e método são unidos diretamente. Qualquer tabela registrada pode receber um `alias`; quando definido, o alias é o nome usado em todo lugar (consultas, SDL, relacionamentos). O nome auto-gerado é a chave de registro e nunca muda. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Mapeamento de tipo (REQ-324).** Tipos escalares proto mapeiam para tipos SQL como segue. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

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

**Relacionamentos no momento do registro.** `relationships` funciona de forma idêntica ao adapter GQL — declara caminhos de join FK/PK armazenados como relacionamentos declarados manualmente (sem flag `remote_managed`). Na atualização, estes são preservados sem alteração. (REQ-554) [tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Métodos de query (REQ-325).** Campos da mensagem de saída se tornam colunas de tabela. Campos da mensagem de entrada se tornam tanto argumentos GraphQL passados à chamada remota *quanto* são registrados como colunas prefixadas com `_nf_` com `native_filter_type: "grpc_input"` — o mesmo mecanismo que GQL e OpenAPI usam para injeção de filtro nativo. (REQ-555) [tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**Sub-campos de mensagem aninhada.** Para métodos de query, campos do tipo mensagem não repetidos na profundidade 0 (colunas de saída diretas) têm seus sub-campos resolvidos um nível mais profundo e armazenados como `object_fields` no `ColumnDef`. Este metadado é usado para extração de sub-campo `jsonb` em SQL e para documentação de esquema. Campos aninhados além da profundidade 1 não são expandidos recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Métodos server-streaming coletam todas as mensagens transmitidas em uma lista antes de retornar linhas. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Métodos de mutação (REQ-326).** Um método de mutação registrado é um command cujos argumentos são os campos da mensagem de entrada, cada um tipado como `json` e repassado sem alteração. A resposta do serviço remoto volta como linhas; uma chamada recusada é um 422, `functions.remote_refused`. Veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). (REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**Gerenciamento de canal.** Um `grpc.aio.Channel` por fonte registrada é armazenado no estado da app e reutilizado através das requisições. O canal antigo é fechado antes que um novo abra na atualização. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Atualização (refresh).** Faça POST para `/admin/grpc-remote/refresh/{source_id}`. Recarrega o proto do caminho armazenado, recompila os stubs e atualiza, conforme o proto, as tabelas já registradas, com as colunas com que cada uma foi registrada. Não registra nenhuma tabela nova; um método de query adicionado ao proto continua oferecido. Como alternativa, faça PUT para `/admin/grpc-remote/{source_id}/proto` com um novo `proto_text` para atualizar o proto inline. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**Limitações.**

- A extração de objeto de sub-campo é de um nível de profundidade. Campos de mensagem aninhados além da profundidade 1 não são expandidos recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Como adicionar a fonte.** Faça POST para `/admin/openapi/register` com um ID de fonte e uma spec, carregada de um arquivo local ou de uma URL. A spec é analisada e mantida com a fonte; nenhuma tabela nem command é registrado. A resposta informa `tables: 0` e `mutations: 0`, com os contadores oferecidos em `available_tables` e `available_mutations`. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**Registrar tabelas.** Registre cada operação GET desejada pelo seletor Register Table (`availableTables`, `availableColumns`, `registerTable`), escolhendo as colunas. Registre cada operação não-GET individualmente como command na página Comandos, listadas por `availableFunctions`; veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). `PUT /admin/openapi/spec/{source_id}` armazena uma spec editada à mão, não registra nada e retorna `available_tables` e `available_mutations`. (REQ-316) [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

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

**O que a fonte oferece.** Toda operação GET da especificação é oferecida como tabela, a menos que seu esquema de resposta seja um tipo escalar (`string`, `number`, `boolean`, `integer`) — operações GET que retornam um escalar são funções com uma única coluna `value`. Toda operação não-GET (POST, PUT, PATCH, DELETE) é oferecida como command, nomeado por seu `operationId`. Registrado, ele recebe os parâmetros de caminho da operação e um argumento `body` para o corpo da requisição, cada um tipado como `json`; qualquer outro argumento vai na query string. Veja [Operação de escrita de uma fonte remota](commands.md#operacao-de-escrita-de-uma-fonte-remota-req-1924). (REQ-316, REQ-317, REQ-1924) [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

Prioridade de classificação: `operation_overrides` (payload) sobrepõe `x-provisa-kind` (extensão de spec) sobrepõe a heurística GET. `operation_overrides` é o caminho de override recomendado; `x-provisa-kind` é para quando a própria spec deve carregar a classificação. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relacionamentos no momento do registro.** `relationships` funciona de forma idêntica aos outros adapters — armazenado como relacionamentos declarados manualmente, preservado na atualização. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Nomeação de tabela.** Tabelas usam o `operationId` da operação. Se nenhum `operationId` for definido, o Provisa faz slugify de `{method}_{path}`. Um alias é derivado removendo o segmento de verbo inicial e singularizando o substantivo (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Mapeamento de tipo.** Tipos JSON Schema mapeiam para tipos Provisa como segue. [tool-verified: `provisa/openapi/register.py:59–70`]

| Tipo JSON Schema | Tipo Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parâmetros como colunas de filtro nativo.** Parâmetros de path e query que ainda não são campos de resposta se tornam colunas com `native_filter_type` definido como `path_param` ou `query_param`, prefixadas com `_nf_`. Quando o nome de um parâmetro corresponde a um nome de campo de resposta, o metadado do parâmetro é mesclado na entrada de coluna existente em vez de criar uma duplicata. (REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**Resolução de esquema de resposta.** O mapper verifica `responses.200`, depois `responses.2xx`, depois `responses.default`. Respostas do tipo array são desempacotadas para o esquema do item. Referências `$ref` são resolvidas um nível de profundidade. (REQ-316) [tool-verified: `provisa/openapi/mapper.py:83–101`]

**Sub-campos de objeto.** Propriedades de resposta com `type: object` e suas próprias `properties` são armazenadas como `object_fields` na coluna. Esses sub-campos são visíveis na SDL e usados para extração `jsonb` em consultas. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Cache de resposta (REQ-318).** Resultados de operação GET são cacheados no PostgreSQL por `pg_cache.py`. Cada combinação de parâmetros de requisição obtém seu próprio grupo `_params_hash`. Linhas para um dado hash são substituídas quando o TTL expira. Endpoints com parâmetro de path (`/pets/{id}`) pulam a busca em massa inicial — a tabela de cache é criada vazia para introspecção de esquema, depois populada por PK conforme as requisições chegam. [tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Atualização (REQ-321).** Faça POST para `/admin/openapi/refresh/{source_id}`. Reanalisa a spec por meio de `_load_and_register`, que não registra nada: não adiciona nenhuma tabela nem coluna. Regras de governança existentes são preservadas. [tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`] Uma tabela registrada mantém suas colunas; ela é lida pela operação da spec atualizada. [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**Limitações.**

- A extração de objeto de sub-campo é de um nível de profundidade. Propriedades aninhadas dentro de `object_fields` não são expandidas recursivamente. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]
- Parâmetros de header e cookie são ignorados; somente parâmetros `path` e `query` são registrados. (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- A resolução de `$ref` em nível de spec é de um nível de profundidade para esquemas de propriedade; referências de componente profundamente aninhadas podem não resolver. [tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## Impacto de registrar uma tabela remota

Uma tabela registrada a partir de qualquer fonte de esquema remoto é uma tabela Provisa de primeira classe. Nada nela é tratado de forma diferente de uma tabela relacional conectada localmente em tempo de execução. (REQ-308, REQ-313)

**Interfaces de consulta.** A tabela é imediatamente consultável via GraphQL, SQL (pgwire ou direto), Cypher (GQL), JSON:API, e Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) A geração de esquema sintetiza `ColumnMetadata` para tabelas remotas já que elas não têm catálogo — o mapeamento de tipo é aplicado no momento da construção do esquema. (REQ-602) [tool-verified: `provisa/api/app.py:1367–1386`]

**Modelo de segurança.** Todas as cinco camadas de governança se aplicam:

1. Controle de acesso a domínio — o `domain_id` da tabela condiciona quais funções conseguem vê-la. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Segurança em nível de linha (RLS) — filtros de linha configurados na tabela são injetados em toda consulta, independentemente da interface. (REQ-040, REQ-041)
3. Visibilidade de coluna — a lista `visible_to` em cada coluna controla a exposição de campo por função. (REQ-039)
4. Mascaramento de coluna — regras de mascaramento se aplicam no Estágio 2 do pipeline de governança. (REQ-040, REQ-263)
5. Guard de predicado — colunas mascaradas são rejeitadas de cláusulas WHERE e HAVING. (REQ-603)

Consultas ad-hoc contra tabelas remotas são permitidas somente sob os direitos do usuário — o acesso é uniformemente baseado em direitos (direitos de tabela/coluna + relacionamentos aprovados), sem modo de governança por tabela. (REQ-001, REQ-003)

**Governança de relacionamento (V002).** Condições JOIN contra tabelas remotas — quando consultadas via SQL ou Cypher — devem corresponder a um relacionamento registrado e aprovado. (REQ-604) A verificação V002 é pulada para consultas GraphQL porque relacionamentos definidos na SDL são pré-aprovados por design. Veja [docs/security.md](security.md#governanca-de-relacionamento-v002).

**Colunas do tipo OBJECT.** Quando uma coluna mapeia para um OBJECT GQL inline não governado ou tipo de objeto OpenAPI, seu tipo Provisa é `jsonb`. A coluna armazena o blob JSON aninhado completo. Quando sub-campos são declarados (`gql_object_fields` ou `object_fields`), o mapa `gql_object_columns` é populado no momento da construção do esquema. O gerador SQL usa esse mapa para emitir expressões de extração `->>` para sub-campos quando uma consulta os seleciona. [tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Args obrigatórios como parâmetros de filtro nativo.** Campos de consulta raiz com args non-null, sem padrão injetam colunas adicionais na tabela registrada. Essas colunas carregam `native_filter_type: query_param`. O tradutor Cypher reescreve `WHERE n.id = $val` para `WHERE n._nf_id = $val`, e o executor GraphQL as recolhe como variáveis para passar ao endpoint remoto. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impacto de criar um relacionamento de cobertura

Quando um steward registra um relacionamento entre duas tabelas remotas (ou entre uma tabela remota e uma tabela local), o relacionamento se torna o caminho de join usado no momento da consulta.

**Como o join prevalece.** Na compilação da consulta, o Provisa resolve o caminho de join através do relacionamento registrado. `source_column` e `target_column` no relacionamento se tornam a condição de join no SQL gerado. O join substitui qualquer chamada remota por tabela que de outra forma seria necessária para o tipo conectado.

**O blob bruto nunca é exposto em SQL.** A coluna `breed` em `petstore__pets` não é selecionável como um valor jsonb bruto em consultas SQL. Quando um relacionamento é registrado entre `petstore__pets` e `petstore__breeds`, consultas SQL percorrem o join — `SELECT breed.name FROM petstore__pets` resolve via o join FK, não um blob. Quando nenhum relacionamento é registrado mas a coluna tem sub-campos declarados (`gql_object_fields`), referências de sub-campo SQL são reescritas para extração `->>` contra o blob armazenado. Este caminho está disponível somente para tipos inline não governados — campos de tipo alvo governado são excluídos inteiramente da SDL e não têm blob do qual extrair. O blob bruto em si nunca é emitido como um valor de coluna nu. [tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

Na SDL GraphQL, um campo OBJECT inline não governado é tipado como o tipo de objeto aninhado. Se é servido por um join ou por extração de blob no momento da execução é um detalhe de implementação — a forma da SDL é idêntica de qualquer forma. Quando o tipo filho é registrado como sua própria tabela (e se torna governado), todas as cinco camadas de governança se aplicam a ele independentemente: suas próprias regras de RLS, visibilidade de coluna, regras de mascaramento, guards de predicado, e controle de acesso a domínio. (REQ-039, REQ-040, REQ-041, REQ-263) A extração de blob ignora isso — os dados do filho chegam pré-embutidos na linha pai e são governados somente pelas regras da tabela pai. Registrar o filho como uma tabela e criar um relacionamento é o caminho para governança de granularidade fina no tipo filho.

**`graphql_alias` no relacionamento.** O campo `graphql_alias` nomeia o campo SDL que o relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e da cardinalidade do relacionamento via `rel_field_name(target.field_name, cardinality)`. (REQ-605) [tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 no caminho de join.** Consultas SQL e Cypher que percorrem o relacionamento estão sujeitas à governança de relacionamento V002. O relacionamento deve ser registrado e aprovado para que o join seja permitido. (REQ-604) A travessia GraphQL via campo de relacionamento SDL é sempre pré-aprovada. [tool-verified: `docs/security.md:41–54`]

**Flag remote-managed.** Relacionamentos auto-detectados durante o registro remoto GraphQL são armazenados com `remote_managed: True`. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:199`] Este é um marcador de metadado; não altera o comportamento de governança.

---

## Comportamento somente-type-def

Nem todo tipo em um esquema remoto precisa ser uma tabela consultável.

Quando `root_table_ids` é definido em um `SchemaInput`, tabelas cujos IDs estão ausentes desse conjunto são excluídas dos campos de consulta raiz na SDL gerada. Elas permanecem presentes como tipos GraphQL e podem ser alcançadas via campos de relacionamento em tabelas que têm entradas raiz. (REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

O mesmo mecanismo se aplica a builds de esquema filtrados por domínio: tabelas em domínios que a função não pode acessar são somente-type-def — sua definição de tipo existe na SDL para travessia de relacionamento, mas nenhum campo de consulta raiz é gerado para elas. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

Uma tabela somente-type-def:

- Não tem campo de consulta raiz — clientes não conseguem consultá-la diretamente pelo nome.
- É alcançável via campos de relacionamento em tabelas que têm entradas raiz.
- Ainda aparece na introspecção de esquema como um tipo nomeado.
- Ainda tem todas as regras de governança aplicadas quando dados são acessados através de um relacionamento. (REQ-039, REQ-040)

A remoção completa do esquema — incluindo a definição de tipo — só acontece quando o registro da tabela é excluído inteiramente. Marcar uma tabela como somente-type-def (removendo seu ID de `root_table_ids` ou filtrando por acesso a domínio) não remove o tipo.

Este design permite que stewards exponham grafos de objeto navegáveis onde alguns tipos são alcançáveis somente por travessia, não por consulta independente.
