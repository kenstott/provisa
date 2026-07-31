# Esquemas Remotos (Remote Schemas)

Uma fonte de esquema remoto conecta uma API externa — GraphQL, gRPC, ou REST (OpenAPI) — à camada
semântica do Provisa. Uma vez registradas, as operações da API externa se tornam tabelas e funções
Provisa de primeira classe. (REQ-308, REQ-316, REQ-325) Toda regra de governança, interface de
consulta, e camada de segurança se aplica automaticamente. (REQ-310, REQ-319, REQ-328) O serviço
remoto nunca vê as regras de governança do Provisa. (REQ-310, REQ-319, REQ-328)

---

## Três tipos de fonte

### Esquema remoto GraphQL (REQ-307–313)

**Como registrar.** Faça POST para `/admin/sources/graphql-remote` com a URL do endpoint, um
namespace, e auth opcional. O Provisa dispara uma consulta de introspecção `__schema` padrão contra
o endpoint remoto. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

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
(REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**Overrides de campo.** `field_overrides` é um mapa `{fieldName: "query" | "mutation"}` aplicado
após a introspecção. Tem prioridade sobre a classificação estrutural. Somente campos do tipo query
podem ser reclassificados como mutations; campos do tipo mutation não têm caminho de override no
GraphQL. (REQ-531) [tool-verified: `provisa/graphql_remote/mapper.py`]

**Relacionamentos no momento do registro.** `relationships` declara caminhos de join FK/PK entre
tabelas no momento do registro. Estes são armazenados como relacionamentos declarados manualmente
(sem flag `remote_managed`). Na atualização, relacionamentos auto-detectados (aqueles com
`remote_managed: True`) são reexecutados e podem mudar; relacionamentos declarados manualmente não
são tocados. (REQ-554) [tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**O que é auto-descoberto.** Todo campo no tipo `Query` remoto que retorna um OBJECT se torna uma
tabela virtual. Todo campo no tipo `Mutation` remoto se torna uma função rastreada. (REQ-308)
[tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Nomeação de tabela.** Tabelas são nomeadas `{namespace}__{field_name}`. Com namespace `petstore`
e um campo de consulta `pets`: o nome da tabela é `petstore__pets`. (REQ-312) [tool-verified:
`provisa/graphql_remote/mapper.py:250`]

**Mapeamento de tipo (REQ-308).** Campos escalares mapeiam diretamente para tipos Provisa. Campos
OBJECT se dividem em dois casos dependendo se o tipo alvo é governado (veja "Tabelas governadas"
abaixo). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`,
`provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Tipo GraphQL | Tipo Provisa |
|---|---|
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT (tipo inline não governado, ex.: `ContactInfo`) | coluna blob `jsonb` |
| OBJECT (tipo alvo governado) | excluído inteiramente da SDL e da busca |
| Qualquer ENUM | `jsonb` |
| Escalar customizado | `text` (padrão) |

**Tabelas governadas.** Um tipo GQL é governado quando aparece como um campo raiz `Query` no
esquema remoto. `_collect_queryable_types` coleta estes durante o registro, preferindo campos sem
argumento obrigatório para que possam ser buscados em massa como alvos de join. [tool-verified:
`provisa/graphql_remote/mapper.py:395–413`]

Quando uma coluna do tipo OBJECT em uma tabela governada aponta para outro tipo governado, essa
coluna está sujeita a três regras simultaneamente [tool-verified:
`provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]:

1. **Excluída da busca GQL** — o campo não é solicitado ao buscar linhas da tabela pai.
2. **Excluída da SDL** — o campo não aparece no tipo pai no esquema gerado.
3. **Acessível somente via um relacionamento declarado** — um steward deve registrar um JOIN entre
   as duas tabelas governadas materializadas. Sem um, o campo simplesmente está ausente; não há
   fallback de blob.

Tipos OBJECT que NÃO são alcançáveis como campos Query raiz (tipos inline como `ContactInfo` ou
`Address`) seguem regras diferentes: são buscados como colunas blob `jsonb` e aparecem na SDL como
campos de objeto aninhado. Sub-campos são acessíveis via extração `-->>` em SQL.

**Argumentos obrigatórios.** Quando um campo de consulta raiz tem argumentos non-null sem valor
padrão, esses se tornam colunas `native_filter_type: query_param` na tabela (prefixadas com `_nf_`
no momento da injeção). O executor as passa como variáveis GraphQL. (REQ-555) [tool-verified:
`provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relacionamentos detectados automaticamente.** O Provisa varre as colunas do tipo OBJECT de cada
tabela. Quando o tipo GQL referenciado também é registrado como uma tabela na mesma fonte, um
relacionamento é emitido. Relacionamentos many-to-one inferem colunas de origem e alvo a partir de
convenções de nomenclatura (`breedName` no tipo de origem → `name` no tipo alvo `Breed`). Campos
one-to-many (LIST) emitem relacionamentos com referências de coluna vazias — a FK vive no lado
alvo. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** Campos de mutation produzem funções rastreadas com tipos de argumento mapeados dos
args da mutation e um `return_schema` derivado do tipo de retorno da mutation. (REQ-308)
[tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Atualização (refresh).** Faça POST para `/admin/sources/graphql-remote/{id}/refresh`.
Reintrospecciona o esquema remoto e atualiza os registros de tabela e função. Regras de governança
existentes (RLS, mascaramento) são preservadas. (REQ-311) [tool-verified:
`provisa/api/admin/graphql_remote_router.py:217–257`]

**Limitações.**
- Campos de consulta raiz escalares e ENUM (tipo de retorno não é OBJECT) se tornam funções
  rastreadas, não tabelas virtuais. Seu `return_schema` é uma única coluna `value` do tipo escalar
  mapeado. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- O aninhamento de objeto é resolvido no momento do registro até `graphql_remote.max_object_depth`
  (padrão: 5). Tanto a seleção de busca remota quanto os metadados de sub-campo são construídos até
  essa profundidade; campos além do limite não são buscados e não estão disponíveis para extração
  SQL. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- Campos OBJECT aninhados do tipo LIST (ex.: `breed.awards: [Award]`) são incluídos na seleção de
  busca até os níveis de aninhamento `graphql_remote.max_list_depth` (padrão: 2). Dentro desse
  limite, a lista é buscada como um array `jsonb` na coluna pai, e a seleção GQL injeta
  `first: N` onde N é `graphql_remote.max_list_items` (padrão: 100) para limitar o tamanho do
  array. Além de `max_list_depth`, o campo LIST é excluído inteiramente para prevenir expansão
  ilimitada de dados. Em SQL, o array é acessado via `json_array_elements(column_name)` ou extração
  de índice `->>`. Se o tipo de item da lista tem sua própria consulta raiz, registre-o como uma
  tabela separada e crie um relacionamento em vez disso — o caminho de join é mais eficiente e
  evita o blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- Para consultas SQL, colunas do tipo OBJECT não governadas são buscadas por completo do remoto
  (todos os sub-campos até a profundidade configurada) e cacheadas como `jsonb`. O acesso a
  sub-campo em SQL é tratado via extração `->>` contra o blob; a requisição remota não é reduzida
  somente aos campos que a consulta SQL seleciona. Quando o tipo de item da LIST não tem consulta
  raiz e a representação em blob é insuficiente, escreva a consulta em SDL GraphQL diretamente — o
  Provisa reproduz fielmente a seleção de campo GQL, para que o remoto veja exatamente os campos
  solicitados. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Se o servidor remoto rejeitar um campo do tipo OBJECT porque requer seleção de sub-campo (o que
  não deveria ocorrer quando `gql_selection` está disponível), o executor tenta novamente uma vez
  com esses campos removidos para que colunas escalares ainda sejam retornadas. [tool-verified:
  `provisa/graphql_remote/executor.py:76–80`]

---

### Esquema remoto gRPC (REQ-322–329)

**Como registrar.** Faça POST para `/admin/grpc-remote/register` com o endereço do servidor, um
caminho ou URL para um arquivo `.proto`, e config TLS opcional.

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

O Provisa busca o proto, o analisa com um parser de texto puro (sem dependências proto externas no
momento da análise), compila stubs Python via `grpc_tools.protoc`, e abre um `grpc.aio.Channel`
persistente. (REQ-322) [tool-verified: `provisa/grpc_remote/loader.py:99–128`,
`provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Arquivos proto também podem ser caminhos locais. Caminhos de importação para tipos bem conhecidos
(`google/protobuf/timestamp.proto`) são armazenados no momento do registro e reutilizados na
atualização. (REQ-329) [tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**O que é auto-descoberto.** Todo método `rpc` no proto é classificado como query ou mutation
usando três sinais em ordem de prioridade: (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** no payload de registro — `{"MethodName": "query"}` ou
   `{"MethodName": "mutation"}` sobrepõe todo o resto.
2. **`server_streaming: true`** — o servidor envia um stream de mensagens; sempre uma tabela
   virtual (a menos que a saída seja um escalar).
3. **A mensagem de saída tem um campo repetido do tipo mensagem** — ex.:
   `ListOrdersResponse { repeated Order items; }` é tratado como um envoltório de lista e se torna
   uma tabela virtual. Campos escalares repetidos (ex.: `repeated string tags`) não disparam isso —
   são propriedades de array em uma única entidade, não fontes de linha.

Métodos que não correspondem a nenhum desses sinais (RPC unário retornando uma única mensagem de
entidade, ou qualquer saída escalar) se tornam funções rastreadas.

**Nomeação de tabela.** O nome padrão é `{namespace}__{ServiceName}__{MethodName}`. Sem um
namespace, os nomes de serviço e método são unidos diretamente. Qualquer tabela registrada pode
receber um `alias`; quando definido, o alias é o nome usado em todo lugar (consultas, SDL,
relacionamentos). O nome auto-gerado é a chave de registro e nunca muda. (REQ-322) [tool-verified:
`provisa/core/repositories/table.py:129–134`]

**Mapeamento de tipo (REQ-324).** Tipos escalares proto mapeiam para tipos SQL como segue.
[tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Tipo Proto | Tipo SQL |
|---|---|
| `string`, `bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| Mensagem aninhada | `jsonb` |
| Enum | `text` |

**Relacionamentos no momento do registro.** `relationships` funciona de forma idêntica ao adapter
GQL — declara caminhos de join FK/PK armazenados como relacionamentos declarados manualmente (sem
flag `remote_managed`). Na atualização, estes são preservados sem alteração. (REQ-554)
[tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Métodos de query (REQ-325).** Campos da mensagem de saída se tornam colunas de tabela. Campos da
mensagem de entrada se tornam tanto argumentos GraphQL passados à chamada remota *quanto* são
registrados como colunas prefixadas com `_nf_` com `native_filter_type: "grpc_input"` — o mesmo
mecanismo que GQL e OpenAPI usam para injeção de filtro nativo. (REQ-555) [tool-verified:
`provisa/api/admin/grpc_remote_router.py:207–213`]

**Sub-campos de mensagem aninhada.** Para métodos de query, campos do tipo mensagem não repetidos
na profundidade 0 (colunas de saída diretas) têm seus sub-campos resolvidos um nível mais profundo
e armazenados como `object_fields` no `ColumnDef`. Este metadado é usado para extração de sub-campo
`jsonb` em SQL e para documentação de esquema. Campos aninhados além da profundidade 1 não são
expandidos recursivamente. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

Métodos server-streaming coletam todas as mensagens transmitidas em uma lista antes de retornar
linhas. (REQ-325) [tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Métodos de mutation (REQ-326).** Campos da mensagem de entrada se tornam argumentos de entrada da
mutation. O esquema da mensagem de saída se torna o `return_schema`. [tool-verified:
`provisa/grpc_remote/executor.py:122–143`]

**Gerenciamento de canal.** Um `grpc.aio.Channel` por fonte registrada é armazenado no estado da
app e reutilizado através das requisições. O canal antigo é fechado antes que um novo abra na
atualização. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Atualização (refresh).** Faça POST para `/admin/grpc-remote/refresh/{source_id}`. Recarrega o
proto do caminho armazenado, recompila stubs, e re-registra tabelas e funções. Alternativamente,
faça PUT para `/admin/grpc-remote/{source_id}/proto` com novo `proto_text` para atualizar o proto
inline. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`,
`provisa/api/admin/grpc_remote_router.py:300–358`]

**Limitações.**
- A extração de objeto de sub-campo é de um nível de profundidade. Campos de mensagem aninhados
  além da profundidade 1 não são expandidos recursivamente. (REQ-556) [tool-verified:
  `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Como registrar.** Chame `auto_register_openapi_source` com um ID de fonte, uma spec analisada, e
metadados de conexão. A spec é carregada de um arquivo local ou URL. (REQ-314) [tool-verified:
`provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

**Payload de registro.** O endpoint `/admin/openapi/register` aceita dois campos adicionais ao
lado de `source_id`, `spec_path`, etc.:

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**O que é auto-descoberto.** Toda operação GET na spec se torna uma tabela virtual, a menos que seu
esquema de resposta seja um tipo escalar (`string`, `number`, `boolean`, `integer`) — GETs que
retornam escalar se tornam funções rastreadas com uma única coluna `value` em vez disso. Toda
operação não-GET (POST, PUT, PATCH, DELETE) se torna uma função rastreada. (REQ-316, REQ-317)

Prioridade de classificação: `operation_overrides` (payload) sobrepõe `x-provisa-kind` (extensão de
spec) sobrepõe a heurística GET. `operation_overrides` é o caminho de override recomendado;
`x-provisa-kind` é para quando a própria spec deve carregar a classificação. (REQ-408)
[tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relacionamentos no momento do registro.** `relationships` funciona de forma idêntica aos outros
adapters — armazenado como relacionamentos declarados manualmente, preservado na atualização.
(REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Nomeação de tabela.** Tabelas usam o `operationId` da operação. Se nenhum `operationId` for
definido, o Provisa faz slugify de `{method}_{path}`. Um alias é derivado removendo o segmento de
verbo inicial e singularizando o substantivo (`findPetsByStatus` → `pet_by_status`). (REQ-557)
[tool-verified: `provisa/openapi/register.py:39–56`]

**Mapeamento de tipo.** Tipos JSON Schema mapeiam para tipos Provisa como segue. [tool-verified:
`provisa/openapi/register.py:59–70`]

| Tipo JSON Schema | Tipo Provisa |
|---|---|
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Parâmetros como colunas de filtro nativo.** Parâmetros de path e query que ainda não são campos
de resposta se tornam colunas com `native_filter_type` definido como `path_param` ou `query_param`,
prefixadas com `_nf_`. Quando o nome de um parâmetro corresponde a um nome de campo de resposta, o
metadado do parâmetro é mesclado na entrada de coluna existente em vez de criar uma duplicata.
(REQ-555) [tool-verified: `provisa/openapi/register.py:116–122`,
`provisa/openapi/register.py:172–196`]

**Resolução de esquema de resposta.** O mapper verifica `responses.200`, depois `responses.2xx`,
depois `responses.default`. Respostas do tipo array são desempacotadas para o esquema do item.
Referências `$ref` são resolvidas um nível de profundidade. (REQ-316) [tool-verified:
`provisa/openapi/mapper.py:83–101`]

**Sub-campos de objeto.** Propriedades de resposta com `type: object` e suas próprias `properties`
são armazenadas como `object_fields` na coluna. Esses sub-campos são visíveis na SDL e usados para
extração `jsonb` em consultas. (REQ-556) [tool-verified: `provisa/openapi/register.py:87–96`]

**Cache de resposta (REQ-318).** Resultados de operação GET são cacheados no PostgreSQL por
`pg_cache.py`. Cada combinação de parâmetros de requisição obtém seu próprio grupo `_params_hash`.
Linhas para um dado hash são substituídas quando o TTL expira. Endpoints com parâmetro de path
(`/pets/{id}`) pulam a busca em massa inicial — a tabela de cache é criada vazia para introspecção
de esquema, depois populada por PK conforme as requisições chegam. [tool-verified:
`provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**Atualização (REQ-321).** Reanalise a spec e chame `auto_register_openapi_source` novamente.
Regras de governança existentes são preservadas; registros são atualizados com upsert ON CONFLICT.
[tool-verified: `provisa/openapi/register.py:249–264`]

**Limitações.**
- A extração de objeto de sub-campo é de um nível de profundidade. Propriedades aninhadas dentro de
  `object_fields` não são expandidas recursivamente. (REQ-556) [tool-verified:
  `provisa/openapi/register.py:87–96`]
- Parâmetros de header e cookie são ignorados; somente parâmetros `path` e `query` são registrados.
  (REQ-555) [tool-verified: `provisa/openapi/mapper.py:144–158`]
- A resolução de `$ref` em nível de spec é de um nível de profundidade para esquemas de propriedade;
  referências de componente profundamente aninhadas podem não resolver. [tool-verified:
  `provisa/openapi/mapper.py:51–60`]

---

## Impacto de registrar uma tabela remota

Uma tabela registrada a partir de qualquer fonte de esquema remoto é uma tabela Provisa de primeira
classe. Nada nela é tratado de forma diferente de uma tabela relacional conectada localmente em
tempo de execução. (REQ-308, REQ-313)

**Interfaces de consulta.** A tabela é imediatamente consultável via GraphQL, SQL (pgwire ou
direto), Cypher (GQL), JSON:API, e Arrow Flight. (REQ-001, REQ-267, REQ-345, REQ-257, REQ-051) A
geração de esquema sintetiza `ColumnMetadata` para tabelas remotas já que elas não têm catálogo — o
mapeamento de tipo é aplicado no momento da construção do esquema. (REQ-602) [tool-verified:
`provisa/api/app.py:1367–1386`]

**Modelo de segurança.** Todas as cinco camadas de governança se aplicam:

1. Controle de acesso a domínio — o `domain_id` da tabela condiciona quais funções conseguem
   vê-la. (REQ-039) [tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. Segurança em nível de linha (RLS) — filtros de linha configurados na tabela são injetados em
   toda consulta, independentemente da interface. (REQ-040, REQ-041)
3. Visibilidade de coluna — a lista `visible_to` em cada coluna controla a exposição de campo por
   função. (REQ-039)
4. Mascaramento de coluna — regras de mascaramento se aplicam no Estágio 2 do pipeline de
   governança. (REQ-040, REQ-263)
5. Guard de predicado — colunas mascaradas são rejeitadas de cláusulas WHERE e HAVING. (REQ-603)

Consultas ad-hoc contra tabelas remotas são permitidas somente sob os direitos do usuário — o
acesso é uniformemente baseado em direitos (direitos de tabela/coluna + relacionamentos aprovados),
sem modo de governança por tabela. (REQ-001, REQ-003)

**Governança de relacionamento (V002).** Condições JOIN contra tabelas remotas — quando consultadas
via SQL ou Cypher — devem corresponder a um relacionamento registrado e aprovado. (REQ-604) A
verificação V002 é pulada para consultas GraphQL porque relacionamentos definidos na SDL são
pré-aprovados por design. Veja [docs/security.md](security.md#governanca-de-relacionamento-v002).

**Colunas do tipo OBJECT.** Quando uma coluna mapeia para um OBJECT GQL inline não governado ou tipo
de objeto OpenAPI, seu tipo Provisa é `jsonb`. A coluna armazena o blob JSON aninhado completo.
Quando sub-campos são declarados (`gql_object_fields` ou `object_fields`), o mapa
`gql_object_columns` é populado no momento da construção do esquema. O gerador SQL usa esse mapa
para emitir expressões de extração `->>` para sub-campos quando uma consulta os seleciona.
[tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**Args obrigatórios como parâmetros de filtro nativo.** Campos de consulta raiz com args non-null,
sem padrão injetam colunas adicionais na tabela registrada. Essas colunas carregam
`native_filter_type: query_param`. O tradutor Cypher reescreve `WHERE n.id = $val` para
`WHERE n._nf_id = $val`, e o executor GraphQL as recolhe como variáveis para passar ao endpoint
remoto. (REQ-555) [tool-verified: `provisa/api/app.py:1280–1303`]

---

## Impacto de criar um relacionamento de cobertura

Quando um steward registra um relacionamento entre duas tabelas remotas (ou entre uma tabela remota
e uma tabela local), o relacionamento se torna o caminho de join usado no momento da consulta.

**Como o join prevalece.** Na compilação da consulta, o Provisa resolve o caminho de join através
do relacionamento registrado. `source_column` e `target_column` no relacionamento se tornam a
condição de join no SQL gerado. O join substitui qualquer chamada remota por tabela que de outra
forma seria necessária para o tipo conectado.

**O blob bruto nunca é exposto em SQL.** A coluna `breed` em `petstore__pets` não é selecionável
como um valor jsonb bruto em consultas SQL. Quando um relacionamento é registrado entre
`petstore__pets` e `petstore__breeds`, consultas SQL percorrem o join — `SELECT breed.name FROM
petstore__pets` resolve via o join FK, não um blob. Quando nenhum relacionamento é registrado mas a
coluna tem sub-campos declarados (`gql_object_fields`), referências de sub-campo SQL são reescritas
para extração `->>` contra o blob armazenado. Este caminho está disponível somente para tipos
inline não governados — campos de tipo alvo governado são excluídos inteiramente da SDL e não têm
blob do qual extrair. O blob bruto em si nunca é emitido como um valor de coluna nu. [tool-verified:
`provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

Na SDL GraphQL, um campo OBJECT inline não governado é tipado como o tipo de objeto aninhado. Se é
servido por um join ou por extração de blob no momento da execução é um detalhe de implementação —
a forma da SDL é idêntica de qualquer forma. Quando o tipo filho é registrado como sua própria
tabela (e se torna governado), todas as cinco camadas de governança se aplicam a ele
independentemente: suas próprias regras de RLS, visibilidade de coluna, regras de mascaramento,
guards de predicado, e controle de acesso a domínio. (REQ-039, REQ-040, REQ-041, REQ-263) A
extração de blob ignora isso — os dados do filho chegam pré-embutidos na linha pai e são governados
somente pelas regras da tabela pai. Registrar o filho como uma tabela e criar um relacionamento é o
caminho para governança de granularidade fina no tipo filho.

**`graphql_alias` no relacionamento.** O campo `graphql_alias` nomeia o campo SDL que o
relacionamento expõe no tipo pai. Quando ausente, o nome é derivado do `field_name` da tabela alvo e
da cardinalidade do relacionamento via `rel_field_name(target.field_name, cardinality)`. (REQ-605)
[tool-verified: `provisa/compiler/schema_gen.py:1050`]

**V002 no caminho de join.** Consultas SQL e Cypher que percorrem o relacionamento estão sujeitas à
governança de relacionamento V002. O relacionamento deve ser registrado e aprovado para que o join
seja permitido. (REQ-604) A travessia GraphQL via campo de relacionamento SDL é sempre
pré-aprovada. [tool-verified: `docs/security.md:41–54`]

**Flag remote-managed.** Relacionamentos auto-detectados durante o registro remoto GraphQL são
armazenados com `remote_managed: True`. (REQ-554) [tool-verified:
`provisa/graphql_remote/mapper.py:199`] Este é um marcador de metadado; não altera o comportamento
de governança.

---

## Comportamento somente-type-def

Nem todo tipo em um esquema remoto precisa ser uma tabela consultável.

Quando `root_table_ids` é definido em um `SchemaInput`, tabelas cujos IDs estão ausentes desse
conjunto são excluídas dos campos de consulta raiz na SDL gerada. Elas permanecem presentes como
tipos GraphQL e podem ser alcançadas via campos de relacionamento em tabelas que têm entradas raiz.
(REQ-601) [tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

O mesmo mecanismo se aplica a builds de esquema filtrados por domínio: tabelas em domínios que a
função não pode acessar são somente-type-def — sua definição de tipo existe na SDL para travessia
de relacionamento, mas nenhum campo de consulta raiz é gerado para elas. (REQ-039) [tool-verified:
`provisa/compiler/schema_gen.py:1068–1076`]

Uma tabela somente-type-def:

- Não tem campo de consulta raiz — clientes não conseguem consultá-la diretamente pelo nome.
- É alcançável via campos de relacionamento em tabelas que têm entradas raiz.
- Ainda aparece na introspecção de esquema como um tipo nomeado.
- Ainda tem todas as regras de governança aplicadas quando dados são acessados através de um
  relacionamento. (REQ-039, REQ-040)

A remoção completa do esquema — incluindo a definição de tipo — só acontece quando o registro da
tabela é excluído inteiramente. Marcar uma tabela como somente-type-def (removendo seu ID de
`root_table_ids` ou filtrando por acesso a domínio) não remove o tipo.

Este design permite que stewards exponham grafos de objeto navegáveis onde alguns tipos são
alcançáveis somente por travessia, não por consulta independente.
