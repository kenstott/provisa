# Referência de API {: #api-reference }

## Visão geral {: #overview }

O Provisa expõe endpoints REST sob dois prefixos: `/data` para execução de consultas e introspecção de esquema, e `/admin` para gerenciamento de configuração. A maioria dos endpoints de dados exige um identificador de função. Operações de configuração de administração usam uma API GraphQL Strawberry em `/admin/graphql`.

---

## Autenticação {: #authentication }

Quando `auth.provider` está configurado em `provisa.yaml`, todos os endpoints exceto `/health` e `/setup/status` exigem um cabeçalho `Authorization: Bearer <token>`.

Sem autenticação configurada, o servidor roda em modo de desenvolvimento. Toda requisição é tratada como a identidade `anonymous`, que mapeia para todas as funções configuradas com acesso a domínio wildcard.

**Login (`POST /auth/login`)** é fornecido pelo provedor de autenticação ativo quando `provider: basic` está configurado. O formato de credencial e a resposta dependem do provedor.

**Introspecção de identidade:**

```http
GET /auth/me
```

Retorna o id, email, nome de exibição, associações a organizações e atribuições de função do usuário autenticado. Em modo de desenvolvimento retorna `dev_mode: true` com todos os IDs de função listados.

```http
GET /auth/provider-type
```

Retorna `{"provider": "<name>"}` ou `{"provider": null}` quando a autenticação não está configurada.

---

## Endpoints de Dados {: #data-endpoints }

### `POST /data/graphql`

Executa uma consulta ou mutação GraphQL.

**Corpo da requisição:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

A requisição é executada com sua função atuante: a função do usuário autenticado ou, no modo de desenvolvimento (sem autenticação), o cabeçalho `X-Provisa-Role`, `org_admin` quando o cabeçalho está ausente. O campo `role` no corpo não seleciona uma função. Se estiver presente, deve ser igual à função atuante; um valor diferente retorna `400` com o código `data.role_mismatch`, nomeando as duas funções. A mesma regra vale para `POST /data/sql`, `POST /data/query` e `POST /query/nl`.

O campo `extensions` suporta o protocolo Automatic Persisted Query (APQ):

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Cabeçalhos:**

- `X-Provisa-Role` — a função com que executar (modo de desenvolvimento); com autenticação, um pedido de função atendido só se ela estiver atribuída ao usuário
- `Accept` — formato de resposta (ver Negociação de Conteúdo)
- `Authorization` — `Bearer <token>` quando a autenticação está habilitada
- `X-Provisa-Redirect-Format` — tipo MIME para saída de redirecionamento S3
- `X-Provisa-Redirect-Threshold` — contagem de linhas acima da qual o redirecionamento é acionado
- `X-Provisa-Redirect` — `true` para forçar o redirecionamento incondicionalmente

**Cache de respostas:** adesão por requisição com a diretiva de operação `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` opcional); a mesma diretiva vale para GraphQL enviado por Arrow Flight. Cypher adere com uma linha `// @provisa cache=true` ou `// @provisa cache_ttl=N`, gRPC com os metadados de chamada `x-provisa-cache: true` ou `x-provisa-cache-ttl: N`. Sem ela, a requisição não lê nem grava o cache de respostas. O cabeçalho de resposta `X-Provisa-Cache` informa `HIT` ou `MISS`. Veja [Cache](configuration.md#per-request-opt-in).

<a id="response-cache-request-headers"></a>

#### Cabeçalhos de requisição do cache de respostas {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, a Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` e `/data/jsonapi` também aceitam a adesão como cabeçalho de requisição. Cada forma abaixo se comporta como `@cached(ttl: N)`:

| Cabeçalho | Equivale a |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (sem valor) | `@cached` (o TTL do operador) |
| `Cache-Control: no-cache` / `no-store` | sem cache, o mesmo que não enviar nada |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

Uma requisição sem um desses cabeçalhos e sem indicação em seu texto não é armazenada em cache, embora caches HTTP simples armazenem por padrão. As configurações de cache do operador continuam valendo como em `@cached`: um cabeçalho nunca torna uma leitura mais recente do que elas permitem, e nunca é recusado por pedir um `ttl` maior. As demais diretivas de `Cache-Control` (`no-transform`, `min-fresh`, ...) são ignoradas, como a RFC 9111 exige de um cache.

Uma requisição pode indicar a adesão mais de uma vez: no texto e em um cabeçalho, ou em dois cabeçalhos. Se concordam, é armazenada em cache uma vez como pedido. Se divergem (por exemplo `@cached(ttl: 60)` com `Cache-Control: max-age=5`, ou `x-provisa-cache-ttl: 10` com `Cache-Control: max-age=20`), a requisição falha com `400` e o código `data.invalid_cache_control`, nomeando cada forma enviada. Um valor `N` que não seja um número inteiro de segundos falha do mesmo modo. Nenhuma forma é preferida à outra.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

**Resposta (JSON inline):**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Resposta (redirecionamento):**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Resposta (multi-raiz com inline/redirecionamento misto):**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Consultas multi-raiz executam cada campo raiz independentemente. Campos abaixo do limite de redirecionamento retornam inline; campos acima redirecionam. A chave `redirects` (plural) mapeia nomes de campo para informações de redirecionamento.

**Cabeçalhos de cache:**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (em HIT)

**Capacidades exigidas:** `QUERY_DEVELOPMENT` para todas as requisições, incluindo introspecção.

---

### Negociação de Conteúdo {: #content-negotiation }

| Cabeçalho Accept | Formato |
| --- | --- |
| `application/json` | JSON (padrão) |
| `application/x-ndjson` | JSON delimitado por nova linha |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### Redirecionamento {: #redirect }

Resultados acima de um limite de linhas configurado (ou quando `X-Provisa-Redirect: true`) são gravados no S3 e uma URL pré-assinada é retornada.

| Formato de Redirecionamento | Gravado por | Memória |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federado | Nenhuma — os dados nunca passam pelo Provisa |
| `application/x-orc` | CTAS federado | Nenhuma — os dados nunca passam pelo Provisa |
| `application/json` | Provisa | Limitado por memória |
| `application/x-ndjson` | Provisa | Limitado por memória |
| `text/csv` | Provisa | Limitado por memória |
| `application/vnd.apache.arrow.stream` | Provisa | Limitado por memória |

Para exportações analíticas grandes, use o redirecionamento para Parquet ou ORC. O motor de federação escreve direto no S3 em paralelo, e nenhum dado passa pelo Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Executa SQL bruto através do pipeline de governança de Estágio 2.

**Corpo da requisição:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (opcional) traz os valores dos marcadores `$1…$n` da instrução, em ordem, separados do seu texto:

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

Os valores são vinculados quando a instrução roda, pelo motor ou pelo driver da fonte, e nunca são escritos na instrução, então um valor não pode mudar o que a instrução é. Eles acompanham exatamente uma instrução cujos marcadores os numeram exatamente. Um lote de instruções, um marcador sem valor ou um valor que nenhum marcador recebe é recusado com `400` e o código `data.sql_parameters_do_not_fit` antes de a instrução ser governada ou executada. Uma instrução sem marcadores é enviada sem `params`, como antes.

**Capacidades exigidas:** `QUERY_DEVELOPMENT`.

Violações de governança em `POST /data/sql` retornam HTTP 403.

**Resposta:** Mesmo formato de `/data/graphql` (linhas JSON por padrão, negociado por conteúdo via `Accept`).

---

### `POST /data/query`

Um único endpoint para toda linguagem de consulta. Ele aceita GraphQL, SQL ou Cypher e detecta qual recebeu.

Consultas Cypher também podem ser enviadas ao endpoint exclusivo Cypher `POST /query/cypher`.

**Corpo da requisição:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Retorna `{"data": ...}` para GraphQL, `{"columns": [...], "rows": [...]}` para SQL e Cypher.

---

### `POST /data/sql/explain`

Explique ou analise uma instrução SQL pelo pipeline governado.

O endpoint envolve o SQL **governado** na sintaxe EXPLAIN do dialeto: a instrução que realmente roda sob o papel de quem chama, depois de RLS e mascaramento. Então o plano que você vê é o da versão autorizada da consulta, não o da entrada bruta.

**Corpo da requisição:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Defina `analyze: true` para executar EXPLAIN ANALYZE. A consulta é executada e o plano traz contagens de linhas e tempos reais. Nem todo dialeto suporta ANALYZE; veja a tabela em [Planos de consulta e estatísticas](engines.md#query-plans-and-statistics).

**Resposta:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` quando o dialeto não tem suporte a EXPLAIN, ou quando `analyze: true` é solicitado em um dialeto que não o suporta (por exemplo, SQLite).

---

### `GET /data/engine/state`

Retorna o estado atual do shard do motor sem acordá-lo.

A UI consulta este endpoint para mostrar um aviso de inicialização enquanto o motor faz partida a frio. Ele nunca acorda o motor, então consultá-lo é seguro e não conta como atividade para o desligamento por ociosidade.

**Resposta:**

```json
{"state": "ready"}
```

Valores possíveis:

| Estado | Significado |
| --- | --- |
| `always-on` | Desktop, auto-hospedado ou coordenador próprio — sem gerenciamento de ciclo de vida |
| `ready` | O shard está no ar e aceitando consultas |
| `starting` | Cold start em andamento |
| `stopped` | O shard está escalado a zero |

---

### `POST /data/engine/prewarm`

Dispara um despertar do motor sem executar uma consulta.

Retorna `202 Accepted` na hora, e o despertar roda em segundo plano. Use quando quiser o motor pronto antes de chegar a primeira consulta, por exemplo a partir de um agendador que roda consultas alguns minutos depois.

**Resposta:** `202 Accepted`, corpo `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simples auto-gerado para cada tabela registrada. A query string mapeia para argumentos GraphQL e a requisição compila e executa através do mesmo pipeline (RLS, mascaramento, roteamento) que o GraphQL.

**Parâmetros de consulta:**

- `limit` — máximo de linhas (≥ 1)
- `offset` — pular linhas (≥ 0)
- `fields` — nomes de campo separados por vírgulas como o esquema GraphQL os escreve, em camelCase na convenção de nomes padrão, por exemplo `?fields=orderId,amount` (padrão: todos os campos escalares). Os nomes de campo de `filter` e `orderBy` seguem a mesma grafia. O JSON:API usa em vez disso o nome físico da coluna (`fields[orders]=order_id`).
- `filter` — array JSON de objetos de filtro `{"field", "comparator", "value"}`. Um valor é lido como o tipo da sua coluna, então `"100"` numa coluna numérica é comparado como 100; um valor que o tipo da coluna não comporta retorna `400`.
- `orderBy` — array JSON de objetos de ordenação `{"field", "direction"}`
- `params` — os parâmetros da tabela como um objeto JSON por nome de parâmetro, ex.: `?params={"region":"EU"}`. Uma tabela registrada sobre uma API, ou uma tabela de consulta, tem um parâmetro para cada entrada que sua origem recebe. Uma leitura sem um parâmetro obrigatório retorna `400` nomeando-o, assim como um nome para o qual a tabela não tem parâmetro. Cada valor é verificado contra o tipo do seu parâmetro.

Valores de filtro e de parâmetro seguem como variáveis ao lado do texto GraphQL que o endpoint monta, então um valor com aspas é comparado como foi escrito.

A função autenticada é exigida; requisições não autenticadas retornam `401`. Uma especificação OpenAPI para essas rotas é servida em `GET /data/rest/openapi.json` com Swagger UI em `GET /data/rest/docs`.

**Cache de respostas:** adesão com um cabeçalho de requisição, `Cache-Control: max-age=N` ou `x-provisa-cache: true` (veja [Cabeçalhos de requisição do cache de respostas](#response-cache-request-headers)). Toda resposta traz `X-Provisa-Cache: HIT|MISS`, e `X-Provisa-Cache-Age` em um acerto.

#### Explorador OpenAPI / Swagger UI {: #openapi-swagger-ui-explorer }

A página do explorador OpenAPI (`/app/openapi`) incorpora o Swagger UI num iframe isolado. A especificação é limitada ao papel, então só aparecem as tabelas e colunas que o papel atual pode ver, e o seletor de domínio pode restringi-la ainda mais. A UI alterna sozinha entre os temas claro e escuro.

A página carrega o HTML da especificação com `fetch()` em vez de um `src` direto do iframe. Assim a requisição leva o token bearer da sessão, e as requisições relativas do próprio Swagger UI se resolvem contra a mesma origem.

Aberta a partir de um link NL "Abrir no OpenAPI", a página expande o endpoint de destino, preenche os parâmetros de consulta a partir da URL gerada pelo NL (ex. `aggregate`, `groupBy`) e clica em Execute, consultando o DOM para que cada etapa termine antes de a próxima começar.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint compatível com [JSON:API](https://jsonapi.org), auto-gerado para cada tabela registrada. Mesmo RLS, mascaramento e roteamento que o GraphQL.

**Cabeçalho `Accept`:** deve incluir `application/vnd.api+json` (o media type JSON:API) ou a requisição retorna `406`.

**Parâmetros de consulta:**

- `fields[<type>]` — conjuntos esparsos de campos, por exemplo `?fields[orders]=amount`. Os nomes são os nomes físicos das colunas, por exemplo `fields[orders]=order_id`, assim como em `filter[...]`, `sort` e nas chaves dos atributos emitidos. O REST usa em vez disso a grafia do GraphQL (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — ex.: `?filter[region]=US`, `?filter[amount][gt]=100`. Um valor é lido como o tipo da sua coluna; um valor que o tipo não comporta retorna `400`.
- `params` — os parâmetros da tabela como um objeto JSON por nome de parâmetro, ex.: `?params={"region":"EU"}`, como no endpoint REST. Um parâmetro obrigatório ausente retorna `400` nomeando-o; um nome desconhecido ou um valor de tipo errado retorna `400` com `source.parameter` igual a `params`. Os links de paginação mantêm o parâmetro.
- `sort` — separado por vírgula, prefixo `-` para descendente, ex.: `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginação
- `page[total]` — `true` conta as linhas que o filtro encontra e retorna `meta.total` e `links.last`. Sem ele, a resposta traz só `links.self`, `links.first`, `links.prev` e `links.next`, e o banco de dados não executa nenhuma contagem. Qualquer valor diferente de `true` ou `false` retorna `400`. Os links de uma resposta mantêm o parâmetro.
- `aggregate` — funções de agregação separadas por vírgula a executar em vez de recuperação de linhas: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Use `?aggregate=count,sum` para solicitar um subconjunto. Respostas de agregação retornam `data: null` com resultados em `meta.aggregate`.
- `groupBy` — nomes de coluna separados por vírgula; usado com `?aggregate=` para agrupar resultados. Apenas colunas no enum `DistinctOnColumn` da tabela são válidas; o servidor retorna `400` para qualquer coluna que a função não possa ver.
- `includeNodes` — `true` para incluir colunas escalares da tabela base (e escalares de dimensão unida nomeados em `include=`) dentro do array `nodes` de cada linha de grupo. Obrigatório quando uma consulta NL de agrupamento também solicita detalhes de dimensão.

- `include` — nomes de relacionamento separados por vírgula, na grafia física, p. ex. `?include=order_items,shipment`: cada recurso relacionado é retornado uma vez em `included`, e cada recurso primário liga o que inclui em `relationships.<name>.data` (uma lista de identificadores de recurso para um relacionamento para-muitos, um identificador ou `null` para um para-um).

As respostas são objetos de recurso com `type`/`id`/`attributes`. O `id` de um recurso é a chave primária da sua tabela: o valor da coluna-chave ou, para uma chave composta, os valores das suas colunas na ordem da chave unidos por `:` (a linha 2 do pedido 7 de uma tabela com chave `(order_id, line)` é `7:2`). Um `:` ou `\` dentro de um valor é escapado com `\`. Uma tabela sem chave primária declarada usa sua coluna `id`; uma tabela sem nenhuma das duas retorna `400`, assim como um `include` que alcança uma. Os erros seguem o formato do objeto de erro JSON:API.

**Cache de respostas:** adesão com um cabeçalho de requisição, `Cache-Control: max-age=N` ou `x-provisa-cache: true` (veja [Cabeçalhos de requisição do cache de respostas](#response-cache-request-headers)). Toda resposta traz `X-Provisa-Cache: HIT|MISS`, e `X-Provisa-Cache-Age` em um acerto. Duas formas divergentes retornam `400` como objeto de erro do JSON:API.

#### Explorador JSON:API {: #jsonapi-explorer }

A página do explorador JSON:API (`/app/jsonapi`) é uma UI de navegador sobre esses endpoints. Selecione uma tabela na lista agrupada por domínio, então configure:

- **Fields** — escolha quais colunas incluir (sparse fieldset); deixe todas desmarcadas para solicitar todas as colunas
- **Relationships** — selecione nomes de relacionamento derivados de FK para carregar via `?include=`
- **Filter** — campo, operador (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), e valor
- **Sort** — um campo, ascendente ou descendente
- **Aggregate** — escolha colunas de agrupamento na lista validada pelo servidor, então marque uma ou mais funções de agregação; quando colunas de agrupamento são selecionadas, uma caixa de seleção "Include nodes" anexa colunas escalares da tabela base a cada linha
- **Page size** — recursos por página, com navegação primeira/anterior/próxima/última

Os resultados são renderizados em uma visão de resumo formatada (cartões de recurso com âncoras de relacionamento clicáveis) ou em uma aba JSON bruto. A URL de requisição ativa é exibida e pode ser copiada. A seleção de tabela e o tamanho de página persistem entre sessões em `localStorage`.

Quando navegado a partir de um link NL "Open in JSON:API", o explorador pré-seleciona a tabela e popula o seletor de agregação a partir dos parâmetros de consulta gerados por NL, então executa a requisição automaticamente.

---

### `POST /query/nl`

Envia uma pergunta em linguagem natural. O serviço inicia um job assíncrono e retorna `202 Accepted` com um `job_id` imediatamente. Requer um provedor LLM configurado na seção de configuração `ai_models`.

**Corpo da requisição:**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Retorna `{"job_id": "<id>"}`. Exceder o limite de taxa de NL por função retorna `429` com um cabeçalho `Retry-After`.

**Recuperar o resultado:**

- `GET /query/nl/{job_id}` — polling. Retorna o documento do job.
- `GET /query/nl/{job_id}/stream` — SSE. Um evento `branch` por alvo de geração conforme completa, seguido de um evento `done`.

Três laços de geração (Cypher, GraphQL, SQL) rodam em paralelo, cada um validado através do compilador e refinado em caso de erro. O prompt é escopado ao esquema visível da função. O documento de resultado chaveia cada ramo por alvo:

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Um ramo que esgota suas iterações devolve `query: null`, `result: null` e uma string `error`. Toda consulta gerada roda com os direitos do consumidor e a governança do estágio 2 aplicada; o serviço nunca contorna a governança.

#### Agrupamento NL com Detalhes de Dimensão {: #nl-group-by-with-dimension-details }

Pense numa consulta NL com group-by que também projeta colunas de uma tabela de dimensão unida, como "contagem de solicitações por usuário com nome de usuário e e-mail". O executor deriva caminhos com pontos por campo (`dim_paths`) das colunas de dimensão do SELECT. Esses caminhos preenchem o parâmetro `includeNodes=` nas URLs geradas para os painéis JSON:API e OpenAPI, para que esses painéis peçam os mesmos campos de dimensão unida que os ramos SQL e GraphQL resolveram. Sem eles, `includeNodes=true` devolveria só os campos escalares da própria tabela agregada base.

No painel gRPC, o `{Type}GroupByRequest` gerado carrega `include_nodes` (bool) e `include` (string repetida de nomes de campo de relacionamento). O `{Type}GroupByRow` retornado inclui um campo `nodes` tipado com as linhas de detalhe de dimensão.

---

### `GET /data/sdl`

Retorna o SDL GraphQL para o esquema de uma função.

**Cabeçalhos:** `X-Provisa-Role: <role_id>` (obrigatório)

**Parâmetros de consulta:**

- `domain` — IDs de domínio separados por vírgula. Quando definido, a resposta é filtrada para o(s) domínio(s) nomeado(s) e tabelas alcançáveis a partir deles.

**Resposta:** SDL GraphQL em `text/plain`.

---

### `GET /data/introspection`

Retorna o JSON de introspecção GraphQL, opcionalmente filtrado por domínio.

**Cabeçalhos:** `X-Provisa-Role: <role_id>` (obrigatório)

**Parâmetros de consulta:** `domain` — IDs de domínio separados por vírgula.

**Resposta:** resultado de introspecção `application/json`.

---

### `GET /data/graph-schema`

Retorna a visão em grafo do esquema da função: rótulos de nó e seus tipos de relacionamento, para clientes Cypher/grafo. Inclui `pk_columns` por rótulo de nó para que os chamadores possam determinar as colunas de chave primária.

**Resposta:** `application/json` com `node_labels` (cada um carregando `pk`/`pk_columns`) e `relationship_types`.

Cada tipo de relacionamento também traz `junction_table_name` e `properties`. Numa aresta de junção, o primeiro nomeia a tabela associativa que ela percorre, e o segundo lista as colunas dessa tabela, legíveis como `r.attr` e filtráveis em `WHERE`. Numa aresta de chave estrangeira o nome é `null` e a lista fica vazia, e é assim que um cliente distingue as duas. A tabela de junção nunca é um rótulo de nó. Ela é a aresta, então não tem etiqueta num cliente de grafos nem linha em `node_labels`.

---

### `GET /data/domains`

Retorna IDs de domínio acessíveis à função requisitante.

**Cabeçalhos:** `X-Provisa-Role: <role_id>` (obrigatório)

**Resposta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Retorna a string de versão do esquema atual. Combina um nonce por boot com um contador de reconstrução. Clientes usam isso para invalidar caches de esquema após reinicializações do servidor.

**Resposta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Retorna o catálogo da função com que a requisição é executada: as tabelas servidas a essa função, como a listagem do Arrow Flight as dá.

**Resposta:** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` é o esquema Arrow da tabela na codificação Arrow IPC: cada coluna com o nome pelo qual o SQL a lê, com seu tipo registrado, sua descrição e seus metadados de chave (`primary_key`, `references`) nos metadados do campo. Comandos e métricas não são listados.

A função é a que a camada de autenticação estabeleceu (`X-Provisa-Role` pode pedir uma função tida, ou um conjunto separado por vírgulas). Uma implantação sem provedor de autenticação lista o catálogo inteiro, como a listagem do Flight. O driver JDBC lê esta rota quando a porta Flight não está acessível.

---

### `GET /data/proto/{role_id}`

Retorna o arquivo `.proto` auto-gerado para uma função.

**Resposta:** esquema protobuf em `text/plain`.

`{role_id}` é uma função que o chamador possui, ou um conjunto de funções possuídas separadas por vírgulas (`analyst,org_admin`), servido como a metafunção do conjunto: a união do que é concedido aos seus membros. Uma função que o chamador não possui é recusada com `403 auth.role_not_assigned`, e um id de metafunção nomeado diretamente com `403 auth.meta_role_named`. `?domains=a,b` restringe o proto a esses domínios; um domínio que a função não alcança é recusado com `403 data.domain_not_accessible`. Uma função sem superfície de dados responde `404 data.no_proto_for_role`, uma função desconhecida `404 data.no_role` e um modelo ainda não construído `503 data.schema_cache_not_ready`.

Cada tabela no esquema da função produz uma `message` proto. Relacionamentos produzem campos de mensagem aninhados. O serviço lista RPCs apenas para as tabelas na raiz do esquema da função, as mesmas do seu tipo `Query` do GraphQL; uma tabela relacionada tem sua mensagem e é alcançada pelo seu relacionamento.

Adicione `?domains=a,b` para restringir o arquivo a esses domínios. O serviço lista então RPCs para as tabelas próprias dos domínios escolhidos; as tabelas relacionadas a elas aparecem como mensagens.

O arquivo acompanha o modelo. Ele é regenerado com o esquema da função sempre que o modelo ou a função muda, inclusive o de uma metafunção. Mapeamento de tipos: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

Stream de Server-Sent Events para notificações de mudança em tempo real a partir de uma tabela.

A entrega de notificações usa um provedor plugável escolhido por tipo de fonte: fontes PostgreSQL usam `LISTEN/NOTIFY` (via asyncpg), fontes MongoDB usam Change Streams (`collection.watch()`), e fontes Kafka usam grupos de consumidores. Cada provedor implementa uma interface comum de observação assíncrona. Filtragem RLS e validação de esquema se aplicam independentemente do provedor. Fontes WebSocket e RSS também são suportadas.

**Cabeçalho — `X-Provisa-Sink`:** Defina para um alvo Kafka (ex.: `kafka://broker:9092/topic`) para redirecionar eventos de mudança para um sink Kafka em vez da resposta SSE. O servidor inicia um consumidor sink e retorna `202 Accepted` em vez de um stream aberto.

### `POST /data/sources/{source_id}/signal`

Informa ao Provisa que uma fonte mudou, sem linhas: o receptor do sinal de mudança `signal`. Cada tabela da fonte cujo sinal de mudança é `signal` (o seu próprio, senão o da sua fonte) é tratada como escrita, como em uma escrita pelo Provisa: suas respostas em cache são descartadas, as views materializadas sobre ela ficam desatualizadas e uma construção da sua réplica é solicitada. A construção de uma tabela protegida contra carga espera sua janela fora de pico. `?table=schema.table` sinaliza uma tabela.

A função do chamador precisa do direito de escrita e deve alcançar o domínio de cada tabela que sinaliza. A resposta é `202 Accepted` com as tabelas sinalizadas:

```json
{"signalled": ["public.orders"]}
```

Uma fonte que não existe, ou uma `table` que ela não tem, responde `404`; uma fonte sem nenhuma tabela no sinal de mudança `signal` responde `409`, porque nada seria atualizado.

---

## Endpoints REST de Administração {: #admin-rest-endpoints }

### Config

#### `GET /admin/config`

Baixa o `provisa.yaml` atual como `application/x-yaml` com um cabeçalho `Content-Disposition: attachment`.

#### `PUT /admin/config`

Envia um YAML de configuração revisado. O servidor grava um backup `.bak`, salva o novo arquivo, recarrega dele as configurações da implantação e o aplica ao model store como um seed explícito e único: o que o arquivo declara é adicionado ou atualizado pelo model store, e nada é removido. Um reinício posterior lê as configurações do arquivo e nunca mais aplica o seu modelo; depois da primeira inicialização, só o model store é dono do modelo.

**Corpo da requisição:** Conteúdo YAML bruto.

**Resposta:**

```json
{"success": true, "message": "Config uploaded and applied"}
```

Em falha de recarregamento: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Baixa o modelo como está, escrito como arquivo de configuração: as configurações do arquivo da implantação com cada seção do modelo lida do model store — stores, regiões, fontes, domínios, funções, produtos de dados, termos do glossário, tabelas e suas colunas, relacionamentos, métricas, tags e suas atribuições, filtros de linha, comandos, webhooks, gatilhos agendados e regras de nomenclatura. Aplicado a um model store vazio, o arquivo constrói o mesmo modelo. O que a implantação semeia em todo store (as fontes embutidas e suas tabelas, os domínios de sistema, `org_admin` e `platform_admin`) fica de fora.

**Parâmetro de consulta:** `sections` (opcional) — uma lista de seções do modelo separadas por vírgulas, por exemplo `sources,tables`. Só essas seções são escritas; um nome desconhecido responde 400 `settings.export_unknown_section`.

O endpoint fica aberto em toda implantação para um chamador com o direito sobre as configurações da implantação; a seção Arquivo de configuração da página de administração o oferece como **Exportar modelo**.

Retorna `application/x-yaml` com `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Retorna os dois lados do diff de configuração — `original` (a linha de base da inicialização) e `current` (o estado vivo) — normalizados de forma idêntica, de modo que a comparação mostre apenas mudanças genuínas, não reordenação ou variação de comentários.

**Resposta:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Gera um patch em diff unificado da linha de base para a configuração enviada.

Envie o YAML revisado como corpo da requisição. A resposta é um arquivo `text/x-patch` (`provisa.config.patch`) que `git apply` ou `patch` consegue consumir diretamente — útil para submeter mudanças de configuração feitas pela UI através de um pipeline de CI/CD.

---

### Settings

#### `GET /admin/settings`

Retorna as configurações atuais da plataforma como JSON.

**Resposta:**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Atualiza as configurações da plataforma em tempo de execução. Todos os campos são opcionais — apenas chaves presentes no corpo são atualizadas.

**Corpo da requisição (exemplo parcial):**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Campos atualizáveis por seção:

- `redirect`: `enabled`, `threshold`, `default_format`, `ttl`
- `sampling`: `default_sample_size`
- `cache`: `default_ttl`
- `naming`: `domain_prefix`, `convention` — grava no arquivo de configuração e aciona recarregamento de esquema
- `relationships`: `auto_track_fk` — governa apenas o rastreamento de chaves estrangeiras. Um relacionamento apoiado em junção é declarado no registro da tabela e nunca é inferido, então esta configuração não o alcança.
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Resposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modelos de IA {: #ai-models }

#### `GET /admin/ai-models`

Retorna as atribuições de modelo de IA da organização atuante, o registro de modelos de vetor e o limite de taxa de NL.

**Resposta:**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

Chaves de API nunca são devolvidas — `api_keys_set` informa apenas se cada fornecedor tem uma chave configurada. As mudanças entram em vigor na próxima requisição; nenhuma reinicialização é necessária.

#### `PUT /admin/ai-models`

Atualiza as atribuições de modelo de IA da organização, o registro de modelos de vetor ou o limite de taxa de NL. Entra em vigor na próxima requisição.

#### `GET /admin/ai-models/vendors/{vendor}/models`

Retorna os nomes de modelo que um fornecedor serve no momento, para o seletor de modelos.

A lista é lida ao vivo da própria API de listagem de modelos do fornecedor usando a chave configurada da organização — ou a credencial da implantação, quando nenhuma chave da organização está definida. Um modelo lançado depois deste build é selecionável no mesmo dia em que o fornecedor o serve.

Retorna `400` quando o fornecedor não publica uma API de listagem de modelos (nesse caso, digite o nome do modelo diretamente) ou quando nenhuma chave está disponível.

---

### Motor de Federação {: #federation-engine }

#### `GET /admin/federation-engine`

Retorna a seleção atual de motor de federação, sua configuração de conexão e o registro completo de motores selecionáveis.

**Resposta:**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

A chave `current` é o motor em execução neste momento; `persisted` é o que está escrito no arquivo de configuração e será carregado na próxima reinicialização. Elas divergem quando a configuração foi alterada mas o serviço ainda não foi reiniciado.

#### `PUT /admin/federation-engine`

Persiste uma seleção de motor de federação.

**Corpo da requisição:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

A seleção é escrita na configuração da plataforma. Ela entra em vigor após a próxima reinicialização do serviço — o motor é escolhido uma vez, na inicialização.

---

### Política de Domínio {: #domain-policy }

#### `POST /admin/domain-policy`

Altera a política de domínio da organização atuante (`use_domains` / `default_domain`).

A mudança não remove nada. Ela é recusada com `409` e o código `settings.domain_policy_catalog_exists` enquanto a organização atuante tiver um catálogo: uma tabela registrada, uma fonte ou um domínio que a implantação não semeou. Os `params` da recusa contam cada tipo (`tables`, `sources`, `domains`). Exclua primeiro esses objetos, cada um pela sua própria ação, e depois mude. Quando um arquivo de configuração declara parte desse catálogo, o código é `settings.domain_policy_catalog_in_config`: a próxima carga o restauraria, então a política é definida no arquivo de configuração.

**Corpo da requisição:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` limpa a sobreposição da organização e volta à configuração no nível da implantação. `use_domains: false` exige `default_domain` (o único nome de domínio em que todas as tabelas caem). A reconstrução do catálogo é síncrona; a resposta retorna quando os esquemas estão prontos.

---

### Observabilidade {: #observability }

#### `GET /admin/traces/recent`

Retorna até N spans concluídos recentes do buffer de spans em memória.

**Parâmetros de consulta:** `limit` (padrão 50, máximo 200)

**Resposta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recarrega a quente um catálogo nomeado no coordenador do motor de federação via sua API REST. Reconecta a conexão interna do Provisa e reexecuta o DDL do OTel.

**Parâmetros de consulta:** `catalog` (padrão `"otel"`)

**Resposta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Reinicia o container do motor de federação (apenas desenvolvimento de nó único).

**Parâmetros de consulta:** `container` (padrão a variável de ambiente `QUERY_ENGINE_CONTAINER`, depois `"trino"`)

---

### Descoberta {: #discovery }

#### `POST /admin/discover/relationships`

Aciona a descoberta de relacionamentos. Sempre executa introspecção de FK a partir do motor de federação. Executa inferência LLM se `ANTHROPIC_API_KEY` estiver definida.

**Corpo da requisição:**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` deve ser um de `"table"`, `"domain"`, `"cross-domain"`. Para escopo `"table"`, `table_id` (inteiro) é obrigatório. Para escopo `"domain"`, `domain_id` é obrigatório.

**Resposta:** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Lista candidatos de relacionamento pendentes.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Aceita um candidato e o registra como relacionamento.

**Corpo da requisição (opcional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejeita um candidato.

**Corpo da requisição:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Retorna a contagem de candidatos rejeitados.

#### `DELETE /admin/discover/candidates/rejected`

Exclui todos os candidatos rejeitados.

---

### Crawl de Fonte {: #source-crawl }

#### `POST /admin/sources/crawl`

Percorre uma fonte de dados para introspectar seu esquema e registrar tabelas.

---

### Busca de Tabela na Fonte {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

Busca tabelas disponíveis (ainda não registradas) em uma fonte por nome.

---

### Perfil de Tabela {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

Executa um perfil de coluna em uma tabela registrada — cardinalidade, mín/máx, taxas de nulo.

---

### Descrições de Fonte {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

Gera descrições assistidas por LLM para as tabelas e colunas de uma fonte.

---

### Armazenamento de Objetos {: #object-storage }

#### `GET /admin/org-storage`

Informa o consumo de armazenamento da organização atuante frente à sua cota de plataforma, e se a organização registrou o próprio armazenamento.

Quando a organização registrou seu próprio DSN, as materializações dela vão para lá e deixam de contar contra a cota. O DSN em si nunca é devolvido.

#### `PUT /admin/org-storage`

Registra (ou limpa) o armazenamento de materialização da própria organização.

**Corpo da requisição:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

O DSN é validado contra o motor de federação antes de ser aceito — um DSN inutilizável falha no registro, e não horas depois durante uma atualização. O valor é criptografado em repouso e nunca é devolvido pelo GET.

Envie `storage_url: null` para limpar o armazenamento próprio da organização e devolver suas materializações ao armazenamento da plataforma (e à cota). O runtime da organização é reconstruído na mesma chamada, então o novo armazenamento passa a valer imediatamente.

---

### Criptografia da Organização {: #org-encryption }

#### `GET /admin/org-encryption`

Retorna o estado atual da chave da organização: impressão digital, id e procedência. Nunca retorna material de chave.

Quando a organização não definiu chave alguma, retorna `{"configured": false}`. Toda organização começa nesse estado e herda a chave da implantação.

#### `PUT /admin/org-encryption`

Define ou rotaciona a chave de criptografia em repouso da organização.

**Corpo da requisição:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omita `key_b64` para que o Provisa gere uma chave — o caminho mais seguro, já que a chave nunca aparece em uma área de transferência ou log de requisição. Fornecer `key_b64` traz sua própria chave.

A rotação adiciona uma nova entrada ativa ao chaveiro e mantém a antiga, de modo que dados escritos sob a chave anterior seguem legíveis. Rotação não é recriptografia. Não há endpoint de exclusão: aposentar a última chave tornaria ilegível todo payload envolvido por ela.

O chaveiro vivo é revinculado na mesma chamada, então a próxima escrita criptografada já usa a nova chave.

---

### Importação Hasura / DDN {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

Converte um arquivo de projeto Hasura v2 ou DDN em configuração proposta do Provisa sem gravar nada.

**Corpo da requisição:**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` é `"auto"` (detectado pela estrutura do arquivo), `"hasura_v2"` ou `"ddn"`.

**Resposta:**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Nada é persistido. O preview não é armazenado em cache no servidor; o `apply` toma o YAML que você fornece, então o que é aplicado é exatamente o que foi revisado (e opcionalmente editado).

#### `POST /admin/import/hasura/apply`

Carrega na organização atuante uma configuração previamente pré-visualizada.

**Corpo da requisição:**

```json
{"config_yaml": "<yaml string>"}
```

Uma importação é um seed explícito e único do model store da organização atuante, a mesma aplicação de `PUT /admin/config`: ela adiciona e atualiza pelo model store o que a configuração importada declara, com proteções e versões, e não remove nada que não mencione. Não existe campo `replace`. Os catálogos do motor das fontes que ela declara, os pools da organização e seus esquemas são reconstruídos antes que a resposta volte.

**Resposta:** `{"summary": {...}}` — as contagens do que a configuração importada declara.

---

### Intercâmbio com o Apache Ossie {: #apache-ossie-interchange }

#### `GET /admin/ossie`

Exporta o modelo governado da organização como um documento YAML do Apache Ossie (incubating).

O documento é derivado do estado vivo a cada requisição — nunca armazenado em cache — portanto não pode ficar desatualizado. Tabelas viram objetos `dataset`, colunas viram objetos `field` e relacionamentos mapeiam para objetos `relationship` do Ossie.

Retorna `text/yaml` com `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

Baixa o catálogo governado como um `sources.yml` do dbt, como ele é servido a uma função.

**Parâmetro de consulta:** `role` — uma função que o chamador tem, ou qualquer função para o detentor de `access_config`.

Retorna `text/yaml` com `Content-Disposition: attachment; filename=provisa.sources.yml`. Uma função desconhecida é um `404` com o código `dbt_sources.unknown_role`. Veja [integrações](integrations.md#dbt-sources-file) para o que o arquivo contém.

#### `POST /admin/ossie/import`

Analisa um documento YAML ou JSON do Ossie e retorna registros propostos de tabela e relacionamento.

**Corpo da requisição:** YAML ou JSON bruto do Ossie. O formato é detectado automaticamente.

**Resposta:**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Nada é registrado. Use a tela de revisão da interface de administração para aceitar ou reduzir as propostas antes que qualquer mutação dispare.

---

### Actions (Funções e Webhooks) {: #actions-functions-and-webhooks }

Todos os endpoints estão sob o prefixo `/admin/actions`.

Toda invocação — de GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, e gRPC Provisa — passa por um único executor governado que aplica a atribuição do comando (`visible_to`), o direito `write` para uma mutação e governança de forma uniforme. Veja [docs/integrations.md](integrations.md#invoking-commands-across-protocols) para a sintaxe de chamada por protocolo.

#### `GET /admin/actions`

Retorna todas as funções de BD e webhooks rastreados.

**Resposta:**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Cada objeto de webhook carrega um booleano `approved`. Um webhook é aprovado assim que um steward executa sua requisição de criação; webhooks declarados em config são auto-aprovados. Um webhook não aprovado é registrado mas não exposto em nenhuma superfície.

#### `POST /admin/actions/functions`

Registra uma função rastreada (comando).

**Campos-chave:**

| Campo | Obrigatório | Descrição |
| --- | --- | --- |
| `name` | Sim | Nome de comando único |
| `kind` | Sim | `"query"` → campo Query GraphQL; `"mutation"` → campo Mutation |
| `implKind` | Não | Como o comando executa — ver tabela abaixo (padrão `source_procedure`) |
| `binding` | Não | Detalhes de conexão específicos de `implKind` (objeto JSON) |
| `returnSchema` | Não | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — torna o comando set-returning em toda superfície |
| `arguments` | Não | Definições de argumento `[{name, type}]`; a ordem posicional importa para chamadores SQL e Bolt |
| `visibleTo` | Não | IDs de função às quais o comando está atribuído (vazio: todas as funções). Uma `mutation` exige também o direito `write` |
| `domainId` | Não | Domínio para posicionamento GraphQL e controle de acesso |

**Valores de `implKind`:**

| `implKind` | O que executa | Campos de `binding` |
| --- | --- | --- |
| `source_procedure` | Stored procedure em uma fonte registrada (padrão) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script do lado do servidor | `script` |
| `http` | Chamada HTTP de saída | `url`, `method` |
| `grpc` | Chamada gRPC de saída a um servidor externo | `target`, `method` |
| `python` | Callable Python hospedado pelo Provisa | `callable` (ex.: `"demo.py_functions:random_dataset"`) |

Os comandos de demonstração `random_python_set` (`implKind: python`) e `random_grpc_set` (`implKind: grpc`) mostram comandos set-returning com `returnSchema` na prática; ambos estão em `config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

Atualiza uma função rastreada pelo nome.

#### `DELETE /admin/actions/functions/{name}`

Exclui uma função rastreada pelo nome.

#### `POST /admin/actions/webhooks`

Registra um webhook rastreado. Registrar ou atualizar um webhook enfileira uma requisição de aprovação de steward — o webhook se torna ativo em todas as superfícies apenas após um steward aprová-lo. Webhooks declarados em config são auto-aprovados. **Campos do corpo da requisição:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

Atualiza um webhook rastreado pelo nome. Qualquer edição redefine a aprovação para pendente até ser reaprovada.

#### `DELETE /admin/actions/webhooks/{name}`

Exclui um webhook rastreado pelo nome.

#### `POST /admin/actions/test`

Testa uma action (função ou webhook) pelo nome.

---

### Funções {: #roles }

Todos os endpoints estão sob o prefixo `/admin/roles`.

| Método | Caminho | Descrição |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Lista todas as funções |
| `POST` | `/admin/roles/` | Cria uma função |
| `PUT` | `/admin/roles/{role_id}` | Atualiza uma função |
| `DELETE` | `/admin/roles/{role_id}` | Exclui uma função |

---

### Usuários {: #users }

Todos os endpoints estão sob o prefixo `/admin/users`.

| Método | Caminho | Descrição |
| --- | --- | --- |
| `POST` | `/admin/users/` | Cria um usuário local |
| `GET` | `/admin/users/` | Lista usuários locais |
| `GET` | `/admin/users/{user_id}` | Obtém um usuário |
| `PUT` | `/admin/users/{user_id}` | Atualiza um usuário |
| `PATCH` | `/admin/users/{user_id}/password` | Altera a senha |
| `DELETE` | `/admin/users/{user_id}` | Exclui um usuário |
| `GET` | `/admin/users/{user_id}/assignments` | Lista atribuições de função |
| `POST` | `/admin/users/{user_id}/assignments` | Adiciona uma atribuição de função |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Remove uma atribuição de função |

---

### Organizações {: #organizations }

Todos os endpoints estão sob `/admin/orgs`.

| Método | Caminho | Descrição |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Lista organizações |
| `POST` | `/admin/orgs/` | Cria uma organização |
| `PUT` | `/admin/orgs/{org_id}` | Atualiza uma organização |
| `DELETE` | `/admin/orgs/{org_id}` | Exclui uma organização |
| `GET` | `/admin/orgs/{org_id}/members` | Lista membros |
| `POST` | `/admin/orgs/{org_id}/members` | Adiciona um membro |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Remove um membro |

---

### Convites {: #invites }

Todos os endpoints estão sob `/admin/invites`.

| Método | Caminho | Descrição |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Cria um convite |
| `GET` | `/admin/invites/` | Lista convites pendentes |
| `DELETE` | `/admin/invites/{token}` | Revoga um convite |

---

### GraphQL de Administração {: #admin-graphql }

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry para todas as operações de administração: CRUD de fontes e tabelas, gerenciamento de relacionamentos, configuração de domínio, regras RLS, controle de cache, convenções de nomenclatura, gerenciamento de tarefas programadas, e compilação de consultas.

Para a referência completa do esquema — todo campo Query, campo Mutation e tipo de entrada/saída — veja a [Referência da API GraphQL de administração](admin-graphql.md).

**Mutações-chave:**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

---

### Setup

#### `GET /setup/status`

Retorna o status de configuração de primeira execução. Sempre não autenticado.

#### `POST /setup/`

Completa a configuração de primeira execução.

---

## Health Check

#### `GET /health` ou `HEAD /health` {: #get-health-or-head-health }

Retorna `{"status": "ok"}`. Sempre não autenticado.

---

## Respostas de Erro {: #error-responses }

| Status | Significado |
| --- | --- |
| 400 | Consulta inválida, erro de validação, ou erro de parse SQL |
| 401 | Token de autenticação ausente ou inválido |
| 403 | Capacidades insuficientes; violação de governança |
| 404 | Função, recurso, ou arquivo de configuração não encontrado |
| 422 | Cabeçalho obrigatório ausente (ex.: `X-Provisa-Role`) |
| 503 | Banco de dados ou fonte não conectado; dependência indisponível |
| 504 | Requisição expirou |

Violações de governança em `POST /data/sql` retornam HTTP 403 com um corpo estruturado:

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Todos os outros erros usam: `{"detail": "<message>"}`.

---

## Endpoint Arrow Flight {: #arrow-flight-endpoint }

Porta `8815`. Transporte colunar Arrow nativo sobre gRPC.

Consultas e descoberta de catálogo estão ambas disponíveis na mesma conexão. O pipeline de governança completo (RLS, mascaramento, amostragem) é aplicado a cada consulta.

A descoberta de catálogo (`list_flights`, `get_flight_info`, `get_schema` e um ticket sem `query`) lista o que está concedido à função do chamador. Com a autenticação ativada, as três chamadas sem ticket tomam a credencial do cabeçalho de chamada `authorization: Bearer` e a função de `x-provisa-role`, e são recusadas sem credencial; veja [Integrações](integrations.md).

**Formato de ticket** (JSON):

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Uso (Python):**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Quando o proxy Zaychik Flight SQL está disponível (porta 8480), os record batches fazem streaming de ponta a ponta sem materialização completa. Recorre à materialização via a camada de consulta federada se o Zaychik não estiver disponível.

**Avisos.** O que uma resposta diz sobre si mesma viaja no fluxo como `app_metadata` de um lote sem linhas: `{"provisa_warnings": [{"code", "params", "message"}]}`. Avisos conhecidos antes de as linhas serem lidas chegam em um lote sem linhas antes delas. Um aviso conhecido só depois de as linhas serem lidas chega em um segundo lote sem linhas, o último do fluxo: `statement.rows_cut` diz que a resposta foi cortada em um limite de linhas e que mais linhas correspondem. Leia `app_metadata` em todo lote, inclusive o último.

---

## Endpoint Protobuf gRPC {: #protobuf-grpc-endpoint }

Porta `50051` (sobreponha com a variável de ambiente `GRPC_PORT` ou a configuração `server.grpc_port`).

Passe a função na chave de metadados gRPC `x-provisa-role`. Se ausente, o servidor aborta com `UNAUTHENTICATED`.

Baixe o proto específico da função em `GET /data/proto/{role_id}`. Apenas tabelas e colunas visíveis a essa função aparecem.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Cada tabela na raiz do esquema da função produz uma RPC de streaming `Query{TypeName}`. São as tabelas que o tipo `Query` do GraphQL da função lista. Uma tabela que a função só alcança através de um relacionamento não tem RPC própria, assim como não tem campo raiz no GraphQL. As RPCs `Insert{TypeName}` existem por simetria de esquema, mas abortam com `UNIMPLEMENTED`.

**Parâmetros.** Uma tabela com parâmetros (uma tabela registrada sobre uma API, ou uma tabela de consulta) tem uma mensagem `{TypeName}Params`, um campo opcional por parâmetro, nomeado sem o prefixo `_nf_`, e `Query{TypeName}Request` a carrega como `params`. Cada valor definido ali chega à origem como o parâmetro, vinculado na instrução; um nome que não é parâmetro da tabela é recusado com `INVALID_ARGUMENT`, e uma leitura sem um parâmetro obrigatório é recusada nomeando-o. O proxy HTTP do gRPC aceita os mesmos valores no objeto `params` do corpo (HTTP 400, código `data.invalid_parameter`, para um nome desconhecido ou um valor de tipo JSON errado).

Um método que não faz parte do serviço da função do chamador responde `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` está habilitado para descoberta de serviços sem um proto pré-compilado. A reflexão descreve a cada chamador o serviço da função com que ele atua: o mesmo `.proto` que essa função baixa de `GET /data/proto/{role_id}`. Ela lista pelo nome o serviço Provisa e o serviço de reflexão, e descreve o arquivo do serviço Provisa.

Uma chamada de reflexão é identificada como qualquer outra chamada gRPC. Com a autenticação ativada, a função vem da credencial do chamador. Com a autenticação desativada, informe a função em `x-provisa-role`; em uma implantação que atende mais de uma organização, informe também a organização em `x-provisa-org`. Uma chamada sem eles é recusada com `UNAUTHENTICATED`. Com a autenticação desativada, a reflexão permanece desabilitada a menos que `server.grpc_allow_unsecured_reflection` esteja definido.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

O servidor gRPC inicia quando o modelo carregado na inicialização define um serviço gRPC. Depois disso, cada chamada é atendida a partir do modelo como ele está: uma tabela registrada ou alterada enquanto o servidor está em execução é atendida e descrita pela chamada seguinte, sem reinício.

#### RPCs de Agregação e Agrupamento {: #aggregate-and-group-by-rpcs }

Quando uma tabela tem `enable_aggregates` definido, o proto gerado inclui dois RPCs adicionais além de `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — retorna escalares de agregação para a tabela (`count`; `sum`, `avg`, `stddev`, `variance` por coluna numérica; `min`, `max` por coluna comparável)
- **`Query{TypeName}GroupBy`** — retorna uma linha por chave de grupo com subcampos de agregação e, opcionalmente, escalares da tabela base e linhas de dimensão unida em um campo `nodes`

Ambos passam pelo mesmo pipeline de agregação do compilador que os campos raiz `{field}_aggregate` e `{field}_group_by` do GraphQL — sem implementação de agregação separada.

**Campo `funcs`.** A mensagem de requisição aceita um campo `funcs` de string repetida. Os valores válidos são `count`, `sum`, `avg`, `stddev`, `variance`, `min`, e `max`. Quando `funcs` é omitido, toda função que o esquema expõe para essa tabela é solicitada. Quando definido, apenas as funções nomeadas aparecem. Se nenhuma das funções nomeadas se aplicar aos tipos de coluna da tabela, a consulta recorre a `count`.

**Campos `include_nodes` e `include`.** Requisições `Query{TypeName}GroupBy` podem definir `include_nodes: true` para incluir colunas escalares da tabela base no campo `nodes` de cada linha. O campo `include` de string repetida nomeia campos de relacionamento muitos-para-um cujas colunas escalares também são aninhadas dentro de `nodes`. Isso corresponde ao comportamento `?includeNodes=` / `?include=` do JSON:API.

---

## Driver JDBC {: #jdbc-driver }

O driver JDBC do Provisa (`provisa-jdbc-<release>.jar`) expõe o catálogo semântico para ferramentas de BI (Tableau, PowerBI, DBeaver).

**URL de conexão:** `jdbc:provisa://host:port`

Domínios mapeiam para esquemas JDBC. Tabelas usam seus aliases registrados. Colunas usam aliases e expõem descrições como `REMARKS`. Métodos de metadados padrão (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) expõem relacionamentos semânticos como metadados PK/FK.

**Suporte SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`.

O driver solicita redirecionamento Arrow IPC por padrão. Resultados fazem streaming batch-by-batch via `ArrowStreamReader`, limitados a um record batch em memória.

---

## Formato do Argumento `orderBy` {: #orderby-argument-format }

O argumento `order_by` usa objetos `{column: direction}` com um enum de direção de 6 valores:

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Direções suportadas: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Subscriptions

Subscriptions SSE estão disponíveis em `GET /data/subscribe/{table}`. A entrega de notificações usa um provedor plugável selecionado por tipo de fonte: fontes PostgreSQL usam `LISTEN/NOTIFY`, fontes MongoDB usam Change Streams, e fontes Kafka usam grupos de consumidores. Filtragem RLS e validação de esquema se aplicam independentemente do provedor. Fontes WebSocket e RSS também são suportadas via o mesmo endpoint.

---

## Glossário de Negócios {: #business-glossary }

O glossário de negócios mapeia nomes de campo físicos — como existem nos bancos de dados de origem — para um vocabulário humano compartilhado. Toda coluna registrada no modelo recebe um termo automaticamente. Nenhuma entrada manual é necessária para popular o glossário; curadores adicionam definições, relacionamentos e especialistas sobre o que o sistema deriva.

### Como os Termos São Derivados {: #how-terms-are-derived }

Quando o Provisa registra ou atualiza as colunas de uma tabela, `normalize_term` (`provisa/core/glossary.py`) roda em cada nome de coluna e produz uma frase canônica.

A normalização aplica cinco regras em sequência:

1. Divide em limites de camelCase e caracteres separadores (`_`, `-`, `.`, `/`, espaço em branco).
2. Converte o resultado para minúsculas.
3. Expande uma tabela de abreviações fixa (ex.: `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Remove um **token proxy** final (`identifier`, `code`, `index`, ou `reference`) — uma coluna nomeada por sua chave ou código está apontando para o conceito subjacente através de um valor substituto, então o termo deve ser o próprio conceito. O último token restante nunca é removido.
5. Qualifica uma **frase genérica demais** com o conceito da tabela. Quando a frase normalizada completa é uma palavra de atributo simples (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, e similares), o termo se torna `<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Um único termo `name` compartilhado entre tabelas não relacionadas mesclaria significados distintos; a qualificação conecta cada coluna ao seu conceito envolvente em vez disso. O conceito da tabela é o nome de negócio da tabela, normalizado com um substantivo núcleo no singular (`order_lines` → `order line`).

Pseudocolunas de filtro nativo (prefixadas com `_nf_`, ou qualquer coluna que carregue `native_filter_type`) são mecanismo de parâmetro de consulta, não campos de negócio, e não derivam termos.

Como `id`, `key`, `pk`, e `sk` todos se expandem para `identifier` antes da verificação de proxy, três nomes de coluna fisicamente diferentes recaem exatamente no mesmo termo:

| Nome físico | Após normalização |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Os três primeiros colapsam em um termo. `transaction amount` mantém ambos os tokens porque `amount` não é um proxy. Uma coluna `id` simples — sem tokens precedentes — não pode ser removida; ela normaliza para `identifier` para que o termo não fique vazio.

### Ciclo de Vida {: #lifecycle }

Termos são **derivados da associação ao modelo**, não criados sob demanda por usuários. O repositório de tabela é o único caminho de escrita: `sync_table_refs` roda dentro de todo upsert de conjunto de colunas, e `sweep_refless_terms` roda após qualquer caminho de exclusão.

**Quando uma coluna é adicionada:** o Provisa busca o termo normalizado pelo nome. Se ele já existir, a coluna recebe uma referência a ele (e se o termo estava depreciado, ele é revivido — `deprecated` é definido de volta para `False`). Se nenhum termo existir ainda, um é criado.

**Quando uma coluna sai** (mudança de esquema ou remoção de tabela): sua referência é excluída e o termo é **resolvido** sob uma regra de remover-ou-depreciar. Um termo enraizado sem referências restantes é removido completamente — junto com suas arestas e atribuições de especialista — a menos que removê-lo deixasse um termo abstrato desconectado de todos os termos enraizados (sem caminho através do grafo de termos). Nesse caso, o termo é **depreciado** (marcado `deprecated=True`) em vez de excluído, para que a âncora de grafo do termo abstrato sobreviva.

Termos abstratos nunca são removidos automaticamente; eles existem fora do ciclo de vida físico e são excluídos apenas explicitamente via a API de administração.

**Revival:** se o nome normalizado de um termo depreciado reaparece (uma coluna é reregistrada), o termo é desmarcado e suas referências voltam a se acumular.

### Endpoints de Curadoria {: #curation-endpoints }

Todos os endpoints estão sob `/admin/glossary`. Eles exigem acesso `org_admin` e uma organização configurada. Toda mutação aciona uma publicação de metadados.

| Método | Caminho | Descrição |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Lista termos. Parâmetros de consulta: `q` (busca por nome/definição), `include_deprecated` (padrão `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Obtém detalhe do termo: definição, referências físicas, arestas tipadas, especialistas |
| `POST` | `/admin/glossary/terms` | Cria um termo abstrato — vocabulário de usuário sem referências físicas |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Renomeia, define a definição, ou alterna a exclusão de exportação |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Exclui um termo que não tem referências físicas |
| `POST` | `/admin/glossary/refs/move` | Move uma referência física para um termo diferente (consolidação) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Adiciona uma aresta de relacionamento tipada entre dois termos |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Remove uma aresta (parâmetros de consulta: `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Marca um usuário como especialista ou autor de um termo |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Remove a designação de especialista/autor de um usuário |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Rascunha uma definição para um termo usando o modelo de IA da organização — retorna apenas texto, nada é persistido até ser salvo |
| `POST` | `/admin/glossary/definitions/generate` | Gera e persiste definições para todo termo que não tenha nenhuma — nunca sobrescreve texto de autoria humana |
| `POST` | `/admin/glossary/relationships/generate` | Propõe e persiste arestas tipadas em todo o glossário usando o modelo de IA da organização |

**Corpo de `POST /admin/glossary/terms`:**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Corpo de `POST /admin/glossary/terms/{term_id}/edges`:**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valores válidos de `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**Corpo de `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valores válidos de `kind`: `expert`, `author`.

**Corpo de `POST /admin/glossary/refs/move`:**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Mover uma referência resolve o termo perdedor sob a regra de remover-ou-depreciar. Use isso para consolidar dois termos que a normalização manteve separados — por exemplo, após uma fonte usar uma abreviação não padronizada que ficou fora da tabela de expansão.

Excluir um termo enraizado (um com referências físicas) retorna `400 glossary.invalid`. Remova ou mova todas as referências primeiro.

**`PATCH /admin/glossary/terms/{term_id}` — campo `export_excluded`:**

```json
{"export_excluded": true}
```

Definir `export_excluded` como `true` retém o termo de todos os snapshots de exportação de metadados, independentemente de suas referências físicas ou status abstrato. Defini-lo de volta para `false` restaura o termo ao snapshot na próxima publicação. Dados de curadoria (definição, arestas, especialistas) não são afetados.

### Curadoria Assistida por IA {: #ai-assisted-curation }

O modelo de IA configurado da organização pode rascunhar definições e propor arestas de relacionamento em todo o glossário em uma única operação. Ambas as ações em lote exigem acesso `org_admin` e uma organização configurada.

**`POST /admin/glossary/definitions/generate`**

Itera por todo termo no glossário, pula qualquer um que já tenha uma definição, e chama o modelo de IA da organização para rascunhar uma para cada termo restante. O rascunho é persistido imediatamente — diferente do endpoint de rascunho por termo (`POST /admin/glossary/terms/{term_id}/definition/generate`), não há etapa de edição. Definições de autoria humana nunca são sobrescritas: a proteção é `if summary["definition"]: continue` antes de qualquer chamada de modelo. Uma notificação de publicação cobre o lote inteiro.

Resposta:

```json
{"generated": 12}
```

`generated` é a contagem de termos que receberam uma nova definição. É zero quando todo termo já tem uma.

**`POST /admin/glossary/relationships/generate`**

Envia a lista completa de termos ao modelo de IA da organização com um prompt que especifica os dez tipos de aresta permitidos (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) e pede apenas propostas confiáveis. O modelo responde com um array JSON; cada entrada é validada antes de qualquer escrita: nomes de termo desconhecidos, auto-arestas, e tipos de aresta fora do enum fechado são descartados silenciosamente. Propostas válidas são inseridas/atualizadas (upsert) de forma idempotente — reexecutar a ação não duplica arestas. Uma notificação de publicação cobre o lote. O endpoint retorna `{"added": 0}` imediatamente quando o glossário contém menos de dois termos não depreciados.

Resposta:

```json
{"added": 5}
```

`added` é a contagem de arestas escritas. Uma aresta que já existia ainda conta — o upsert é bem-sucedido, mas os dados da aresta não mudam.

### Ferramenta MCP `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

Busca nomes e definições de termos com correspondência de substring sem distinção de maiúsculas/minúsculas, até `limit` resultados. Cada resultado é o detalhe completo do termo: `name`, `definition`, `is_abstract`, `deprecated`, referências físicas (com `source_id`, `schema_name`, `table_name`, `column_name`), arestas tipadas, e atribuições de especialista.

Use `search_terms` antes de escrever SQL para encontrar todo campo físico que representa um conceito por nome. Por exemplo, buscar `"order date"` retorna o termo e todas as colunas `order_dt`, `orderDate`, `ORDER_DATE` em toda tabela registrada.

### Ferramentas de estatística do MCP {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

Cada ferramenta recebe um conjunto de dados definido por `sql` e o executa pelo mesmo pipeline governado que `run_sql`, com o papel de quem chama: aplicam-se as regras de linha, o mascaramento e a ocultação, o SQL que o pipeline recusa falha como em `run_sql`, e um papel que quem chama não detém é recusado. Cada uma devolve resultados, nunca as linhas, com o método usado e o `n` contado.

| Ferramenta | Devolve |
| --- | --- |
| `describe` | Por coluna: `count`, `missing`, `distinct`; para um número `mean`, `sd`, `min`, `max` e `quantiles` (p0, p5, p25, p50, p75, p95, p100); `top_values` (os cinco mais frequentes) |
| `correlate` | A `matrix` de correlação `pearson` ou `spearman` das `columns` numéricas, sobre as linhas completas em todas elas, e esse `n` |
| `regress` | `kind="linear"`: `coefficients` de mínimos quadrados (`estimate`, `se`, `p_value`), `r_squared`, `adj_r_squared`. `kind="logistic"`: um `target` de dois valores, `coefficients` de máxima verossimilhança, `pseudo_r_squared`, `auc` e o `positive_value`. Uma variável de texto vira um indicador por valor, exceto o primeiro |
| `compare` | O `n`, a `mean` e o `sd` de cada grupo; para dois grupos, o teste t de Welch (`t`, `df`, `p_value`) e o d de Cohen; para mais, ANOVA de um fator (`f`, `df_between`, `df_within`, `p_value`) e eta ao quadrado |
| `forecast` | `horizon` pontos após o último `time`, cada um com `value`, `lower` e `upper` (intervalo de predição de 95%), por suavização exponencial (ETS) com erro aditivo, tendência aditiva a partir de 10 pontos e sazonalidade aditiva quando `seasonal_periods` é informado |

O mecanismo calcula o que pode sobre cada linha que o papel lê: as contagens, contagens distintas, médias, desvios-padrão, mínimos e máximos de `describe`, e o `n`, a média e a variância de cada grupo para `compare`. Um modelo ajustado e os quantis e valores mais frequentes de `describe` leem no máximo `mcp.stats_max_rows` linhas (Admin → Settings → Limits; 100.000 por padrão). Toda resposta que lê linhas informa `rows_read`, `row_cap` e `truncated`, e o limite de linhas do próprio papel é relatado em `warnings`, como em `run_sql`.

As ferramentas de estatística devolvem apenas resultados, então as linhas por trás de uma análise nunca entram no contexto do modelo: minimização de dados por padrão. É o controle de acesso que limita o que um agente pode saber. Para manter identidades fora do alcance de um agente, conceda ao papel dele apenas as colunas que não identificam ninguém; um papel que pode ler uma coluna pode agregá-la, e agregados sobre grupos pequenos ainda podem identificar pessoas. Veja [Segurança](security.md#statistics-over-mcp).

### Exportação de Metadados {: #metadata-export }

O grafo de termos do glossário é incluído em todo `MetadataSnapshot` construído por `build_snapshot`.

A exportação aplica os mesmos filtros que o restante do snapshot:

- Um termo marcado `export_excluded` é retido completamente — independentemente de suas referências físicas, status abstrato, ou se o catálogo da organização está configurado.
- Um termo enraizado publica apenas quando pelo menos uma de suas referências físicas pertence a uma coluna que passa tanto pelo filtro de **Data Product** (a flag `data_product` da tabela deve ser `true`) quanto pelo filtro de coluna **técnica** (colunas marcadas como `technical` são retidas).
- Um termo enraizado cujas referências são todas retidas por esses filtros é retido junto com elas.
- Termos abstratos publicam incondicionalmente — eles são vocabulário de usuário, não vinculado a colunas físicas.
- Uma aresta entre dois termos publica apenas quando ambos os termos das extremidades publicam.

Todo adaptador de fornecedor publica o grafo de termos nativamente, em um contêiner de glossário de propriedade do Provisa que ele cria de forma idempotente — nunca em um glossário de catálogo existente:

| Provedor | Contêiner | Termos | Relações | Depreciação |
| --- | --- | --- | --- | --- |
| Apache Atlas | "Provisa Glossary" (API de glossário) | termos de glossário, definição em `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marcador `[DEPRECATED]` shortDescription |
| Atlan | Glossário Provisa por qualifiedName estável | `longDescription` (nunca o `userDescription` editado por humanos) | mesmo mapeamento Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspecto `glossaryTermInfo` por termo | KIND_OF → Inherits, PART_OF → Contains (invertido), RELATED_TO/SYNONYM_OF → termos relacionados | aspecto de depreciação; renomeações seguem sucessão de URN |
| OpenMetadata | Glossário Provisa via `/v1/glossaries` | PUT chaveado por fqn, renomeações PATCH-rebind por UUID armazenado | KIND_OF → hierarquia pai nativa, SYNONYM_OF → `synonyms`, outros → `relatedTerms` | `entityStatus` |
| Collibra | Domínio tipo Glossário "Provisa Glossary" | ativos Business Term via a Import API | tipos de relação Business Term nativos | status do ativo |

A propriedade é o vínculo, não o nome: o id de fornecedor de cada termo publicado é capturado em `catalog_bindings` sob o URN do termo (`provisa://<org>/terms/<name>`), e o Provisa modifica ou exclui um item de glossário do lado do fornecedor apenas quando detém esse vínculo (ou o item vive no contêiner de propriedade do Provisa que ele criou). Um item de glossário sem vínculo Provisa se originou no sistema externo e nunca é tocado; atualizações fazem read-merge para que campos adicionados por stewards nos próprios termos do Provisa sobrevivam; nada é excluído quando um termo sai do snapshot. Atribuições de termo-para-ativo feitas por stewards permanecem de propriedade externa — nenhum adaptador escreve atribuições de termo-para-ativo (a publicação de atribuições de autoria do Provisa é um follow-on explícito). Especificamente no Collibra, a segurança sob a semântica REPLACE da Import API repousa na contenção: o payload menciona apenas ativos dentro do domínio de glossário Provisa e instâncias de relação apenas entre termos Provisa, de forma que glossários de stewards e suas relações nunca são alcançáveis.

---

## Produtos de dados {: #data-products }

Um produto de dados agrupa tabelas publicadas juntas para consumo, possuído por exatamente um domínio. Os campos seguem o vocabulário do ODPS (Open Data Product Standard) onde o Provisa já detém a fonte da verdade. A interface de administração expõe Produtos de Dados em **Admin → Data Products**.

### Capabilities

| Capability | Concede |
| --- | --- |
| `data_product_read` | Acesso de leitura ao campo de consulta `data_products` e à página de administração de Produtos de Dados. Atribuído por padrão a `org_admin`, `analyst`, `developer` e `modeler`. |
| `data_product_rw` | Mutações de criação e exclusão. Habilita os controles Novo / Editar / Excluir na interface. |

### Admin GraphQL {: #admin-graphql_1 }

Todas as operações de produto de dados passam por `POST /admin/graphql`.

**Consulta:**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Exige `data_product_read`.

**Criar ou atualizar:**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` faz upsert — chamá-lo com um `id` existente atualiza o registro. Exige `data_product_rw`.

**Excluir:**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Excluir um produto limpa `product_id` de toda tabela membro, removendo sua associação. Exige `data_product_rw`.

### Esquema de campos {: #field-schema }

| Campo | Tipo | Obrigatório | Notas |
| --- | --- | --- | --- |
| `id` | `String` | Sim | Identificador estável legível por máquina, ex. `customer_360` |
| `domain_id` | `String` | Sim | Domínio proprietário. Tabelas membro devem compartilhar este `domain_id` — divergências são rejeitadas no salvamento |
| `name` | `String` | Sim | Nome de exibição |
| `owner_role` | `String` | Não | Função responsável por este produto; distinta do steward do domínio |
| `team_role` | `String` | Não | Função cujos membros mantêm este produto no dia a dia; resolve para indivíduos |
| `purpose` | `String` | Não | O que este produto publica e por quê |
| `limitations` | `String` | Não | Restrições, ressalvas ou exclusões conhecidas |
| `usage` | `String` | Não | Como consumir este produto |
| `version` | `String` | Não | ex. `1.2.0` |
| `status` | `String` | Não | ex. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Não | Compromissos de nível de serviço; texto — um produto abrange múltiplas tabelas e um SLA estruturado não consegue nomear sem ambiguidade qual membro descreve |
| `support` | `String` | Não | Orientação de suporte em texto livre |
| `custom_properties` | `JSON` | Não | Metadados arbitrários de chave-valor não cobertos pelos campos padrão |

Dois campos adicionais existem no modelo mas não são expostos no `DataProductType` / `DataProductInput` do Strawberry — são específicos do Snowflake Horizon Catalog:

| Campo | Notas |
| --- | --- |
| `support_contact` | Email ou URL; exigido pelos manifestos de listagem da organização do Horizon Catalog |
| `publish` | `true` para publicar listagens do Horizon imediatamente; novas listagens padrão como DRAFT |

### Associação de tabelas {: #table-membership }

Uma tabela integra um produto de dados ao definir seu campo `product_id` no formulário de edição de tabela. O seletor é limitado a produtos cujo `domain_id` corresponda ao domínio da própria tabela — uma tabela no domínio `marketing` nunca recebe como opção um produto no domínio `sales`.

Comandos do mesmo domínio também podem ser atribuídos como membros.

### Filtro de exportação de metadados {: #metadata-export-filter }

`build_snapshot` aplica `data_products_only=True` para toda publicação em catálogo. Tabelas sem `product_id` são retidas do snapshot, juntamente com suas arestas de relacionamento, arestas de linhagem e tags de governança. Fontes e domínios sempre são publicados. Termos do glossário são publicados somente quando pelo menos uma de suas referências físicas pertence a uma tabela exportada (membro de produto).

Um produto sem membros exportados não gera uma entrada no snapshot — uma listagem sem membros representaria erroneamente o produto ao catálogo.

### Suporte a produto de dados por destino de catálogo {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` alcança todo adaptador, mas somente adaptadores cuja plataforma tem um conceito nativo de produto de dados o publicam como entidade de primeira classe; os demais publicam as tabelas membro (já filtradas acima) sem agrupamento por produto.

| Destino | Representação do produto de dados |
| --- | --- |
| Snowflake Horizon | Cada produto vira um `SHARE` sobre os endereços físicos das tabelas membro, encapsulado em um `CREATE ORGANIZATION LISTING` interno — um Data Product nativo do Horizon Catalog. `publish=true` torna a listagem ativa imediatamente; caso contrário, fica como DRAFT. |
| BigQuery Dataplex | Cada produto vira uma listagem do Analytics Hub via `/v1/dataProducts`. |
| OpenMetadata | Cada produto vira uma entidade nativa `DataProduct` (`/api/v1/dataProducts`), com propriedade derivada do domínio. |
| DataHub | Cada produto vira uma entidade nativa `dataProduct` (`urn:li:dataProduct:...`) com seus próprios aspectos `dataProductProperties`/propriedade. |
| Collibra | Cada produto vira um ativo do tipo de comunidade `Data Product`, relacionado às suas tabelas membro via uma relação `Data Product groups Table`. |
| Atlan | Publicado como suposição de typedef personalizado `DataProduct` — o Atlan não tem nome de tipo documentado e estável para este conceito, portanto o mapeamento é de melhor esforço. |
| Apache Atlas | Publicado como typedef personalizado `provisa_data_product` com uma relação `provisa_data_product_members` — o Atlas não tem tipo de entidade nativo para produto de dados. |
| OpenLineage | Não é uma entidade de primeira classe — tabelas membro carregam uma faceta personalizada `provisa_data_product` nomeando o produto proprietário. |
