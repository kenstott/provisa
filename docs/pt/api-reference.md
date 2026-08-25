# Referência de API

## Visão geral

O Provisa expõe endpoints REST sob dois prefixos: `/data` para execução de consultas e introspecção de esquema, e `/admin` para gerenciamento de configuração. (REQ-043) A maioria dos endpoints de dados exige um identificador de função. Operações de configuração de administração usam uma API GraphQL Strawberry em `/admin/graphql`. (REQ-164)

---

## Autenticação

Quando `auth.provider` está configurado em `provisa.yaml`, todos os endpoints exceto `/health` e `/setup/status` exigem um cabeçalho `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sem autenticação configurada, o servidor roda em modo de desenvolvimento. Toda requisição é tratada como a identidade `anonymous`, que mapeia para todas as funções configuradas com acesso a domínio wildcard. (REQ-535)

**Login (`POST /auth/login`)** é fornecido pelo provedor de autenticação ativo quando `provider: basic` está configurado. (REQ-124) O formato de credencial e a resposta dependem do provedor.

**Introspecção de identidade:**

```http
GET /auth/me
```

Retorna o id, email, nome de exibição, associações a organizações e atribuições de função do usuário autenticado. Em modo de desenvolvimento retorna `dev_mode: true` com todos os IDs de função listados. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Retorna `{"provider": "<name>"}` ou `{"provider": null}` quando a autenticação não está configurada. [tool-verified: `provisa/api/auth_router.py`]

---

## Endpoints de Dados

### `POST /data/graphql`

Executa uma consulta ou mutação GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Corpo da requisição:**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

O campo `role` é usado apenas em modo de desenvolvimento (sem autenticação). Quando a autenticação está ativa, a função do usuário autenticado é usada e o `role` no corpo é ignorado.

O campo `extensions` suporta o protocolo Automatic Persisted Query (APQ): (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**Cabeçalhos:**

- `X-Provisa-Role` — sobrepõe a função (modo de desenvolvimento)
- `Accept` — formato de resposta (ver Negociação de Conteúdo)
- `Authorization` — `Bearer <token>` quando a autenticação está habilitada
- `X-Provisa-Redirect-Format` — tipo MIME para saída de redirecionamento S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — contagem de linhas acima da qual o redirecionamento é acionado (REQ-137)
- `X-Provisa-Redirect` — `true` para forçar o redirecionamento incondicionalmente (REQ-029)

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

Consultas multi-raiz executam cada campo raiz independentemente. Campos abaixo do limite de redirecionamento retornam inline; campos acima redirecionam. A chave `redirects` (plural) mapeia nomes de campo para informações de redirecionamento. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**Cabeçalhos de cache:**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (em HIT) (REQ-536)

**Capacidades exigidas:** `QUERY_DEVELOPMENT` para todas as requisições, incluindo introspecção. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Negociação de Conteúdo

| Cabeçalho Accept | Formato |
| --- | --- |
| `application/json` | JSON (padrão) |
| `application/x-ndjson` | JSON delimitado por nova linha |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirecionamento

Resultados acima de um limite de linhas configurado (ou quando `X-Provisa-Redirect: true`) são gravados no S3 e uma URL pré-assinada é retornada. (REQ-029, REQ-044)

| Formato de Redirecionamento | Gravado por | Memória |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS federado | Nenhuma — os dados nunca passam pelo Provisa |
| `application/x-orc` | CTAS federado | Nenhuma — os dados nunca passam pelo Provisa |
| `application/json` | Provisa | Limitado por memória |
| `application/x-ndjson` | Provisa | Limitado por memória |
| `text/csv` | Provisa | Limitado por memória |
| `application/vnd.apache.arrow.stream` | Provisa | Limitado por memória |

Para grandes exportações analíticas, use redirecionamento Parquet ou ORC. O motor de federação grava diretamente no S3 em paralelo — nenhum dado passa pelo Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Executa SQL bruto através do pipeline de governança de Estágio 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Corpo da requisição:**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capacidades exigidas:** `QUERY_DEVELOPMENT`.

Violações de governança em `POST /data/sql` retornam HTTP 403. (REQ-002, REQ-266)

**Resposta:** Mesmo formato de `/data/graphql` (linhas JSON por padrão, negociado por conteúdo via `Accept`).

---

### `POST /data/query`

Endpoint de consulta unificado. Aceita GraphQL, SQL ou Cypher — a sintaxe é auto-detectada. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Consultas Cypher também podem ser enviadas ao endpoint exclusivo Cypher `POST /query/cypher`. (REQ-345)

**Corpo da requisição:**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Retorna `{"data": ...}` para GraphQL, `{"columns": [...], "rows": [...]}` para SQL e Cypher.

---

### `POST /data/sql/explain`

Explique ou analise uma instrução SQL pelo pipeline governado. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

O endpoint envolve o SQL **governado** — a instrução que de fato roda sob a função de quem chama, depois de RLS e mascaramento — na sintaxe EXPLAIN do dialeto. O que o plano mostra é a versão autorizada da consulta, não a entrada bruta.

**Corpo da requisição:**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Defina `analyze: true` para executar EXPLAIN ANALYZE. A consulta é executada e o plano traz contagens de linhas e tempos reais. Nem todo dialeto suporta ANALYZE; veja a tabela em [Planos de consulta e estatísticas](engines.md#query-plans-and-statistics).

**Resposta:** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` quando o dialeto não tem suporte a EXPLAIN, ou quando `analyze: true` é solicitado em um dialeto que não o suporta (por exemplo, SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Retorna o estado atual do shard do motor sem acordá-lo. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

A UI consulta este endpoint para exibir um aviso de inicialização enquanto o motor faz cold start. Ele nunca dispara um despertar — a consulta é segura e não conta como atividade para o coletor de ociosidade.

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

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Dispara um despertar do motor sem executar uma consulta. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Retorna `202 Accepted` imediatamente. O despertar roda em segundo plano. Use isto se quiser o motor pronto antes que a primeira consulta chegue — por exemplo, a partir de um agendador que executa consultas alguns minutos depois.

**Resposta:** `202 Accepted`, corpo `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simples auto-gerado para cada tabela registrada. A query string mapeia para argumentos GraphQL e a requisição compila e executa através do mesmo pipeline (RLS, mascaramento, roteamento) que o GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Parâmetros de consulta:**

- `limit` — máximo de linhas (≥ 1)
- `offset` — pular linhas (≥ 0)
- `fields` — nomes de coluna separados por vírgula (padrão: todos os campos escalares)
- `filter` — array JSON de objetos de filtro `{"field", "comparator", "value"}`
- `orderBy` — array JSON de objetos de ordenação `{"field", "direction"}`

A função autenticada é exigida; requisições não autenticadas retornam `401`. Uma especificação OpenAPI para essas rotas é servida em `GET /data/rest/openapi.json` com Swagger UI em `GET /data/rest/docs`.

#### Explorador OpenAPI / Swagger UI

A página do explorador OpenAPI (`/app/openapi`) incorpora o Swagger UI em um iframe isolado (sandboxed). A especificação é restrita por função — apenas tabelas e colunas visíveis à função atual aparecem — e opcionalmente filtrada por domínio via o seletor de domínio. A UI alterna entre os temas claro e escuro automaticamente. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

A página carrega o HTML da especificação via `fetch()` em vez de um `src` de iframe direto, de forma que a requisição carrega o token bearer da sessão e as próprias requisições relativas do Swagger UI são resolvidas corretamente contra a mesma origem. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Quando navegado a partir de um link NL "Open in OpenAPI", a página expande automaticamente o endpoint alvo, popula os parâmetros de consulta a partir da URL gerada por NL (ex.: `aggregate`, `groupBy`), e clica em Execute — usando polling do DOM para garantir que cada etapa seja concluída antes que a próxima seja acionada. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint compatível com [JSON:API](https://jsonapi.org), auto-gerado para cada tabela registrada. Mesmo RLS, mascaramento e roteamento que o GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Cabeçalho `Accept`:** deve incluir `application/vnd.api+json` (o media type JSON:API) ou a requisição retorna `406`.

**Parâmetros de consulta:**

- `fields[<type>]` — sparse fieldsets, ex.: `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — ex.: `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separado por vírgula, prefixo `-` para descendente, ex.: `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginação
- `aggregate` — funções de agregação separadas por vírgula a executar em vez de recuperação de linhas: `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Use `?aggregate=count,sum` para solicitar um subconjunto. Respostas de agregação retornam `data: null` com resultados em `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — nomes de coluna separados por vírgula; usado com `?aggregate=` para agrupar resultados. Apenas colunas no enum `DistinctOnColumn` da tabela são válidas; o servidor retorna `400` para qualquer coluna que a função não possa ver. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` para incluir colunas escalares da tabela base (e escalares de dimensão unida nomeados em `include=`) dentro do array `nodes` de cada linha de grupo. Obrigatório quando uma consulta NL de agrupamento também solicita detalhes de dimensão. (REQ-1405)

Respostas são objetos de recurso com `type`/`id`/`attributes`. Erros seguem o formato de objeto de erro do JSON:API.

#### Explorador JSON:API

A página do explorador JSON:API (`/app/jsonapi`) é uma UI de navegador sobre esses endpoints. Selecione uma tabela na lista agrupada por domínio, então configure:

- **Fields** — escolha quais colunas incluir (sparse fieldset); deixe todas desmarcadas para solicitar todas as colunas
- **Relationships** — selecione nomes de relacionamento derivados de FK para carregar via `?include=`
- **Filter** — campo, operador (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), e valor
- **Sort** — um campo, ascendente ou descendente
- **Aggregate** — escolha colunas de agrupamento na lista validada pelo servidor, então marque uma ou mais funções de agregação; quando colunas de agrupamento são selecionadas, uma caixa de seleção "Include nodes" anexa colunas escalares da tabela base a cada linha
- **Page size** — recursos por página, com navegação primeira/anterior/próxima/última

Os resultados são renderizados em uma visão de resumo formatada (cartões de recurso com âncoras de relacionamento clicáveis) ou em uma aba JSON bruto. A URL de requisição ativa é exibida e pode ser copiada. A seleção de tabela e o tamanho de página persistem entre sessões em `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Quando navegado a partir de um link NL "Open in JSON:API", o explorador pré-seleciona a tabela e popula o seletor de agregação a partir dos parâmetros de consulta gerados por NL, então executa a requisição automaticamente. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Envia uma pergunta em linguagem natural. O serviço inicia um job assíncrono e retorna `202 Accepted` com um `job_id` imediatamente. Requer um provedor LLM configurado na seção de configuração `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corpo da requisição:**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Retorna `{"job_id": "<id>"}`. Exceder o limite de taxa de NL por função retorna `429` com um cabeçalho `Retry-After`. (REQ-370)

**Recuperar o resultado:**

- `GET /query/nl/{job_id}` — polling. Retorna o documento do job.
- `GET /query/nl/{job_id}/stream` — SSE. Um evento `branch` por alvo de geração conforme completa, seguido de um evento `done`. (REQ-357, REQ-358)

Três laços de geração (Cypher, GraphQL, SQL) rodam em paralelo, cada um validado através do compilador e refinado em caso de erro. (REQ-355) O prompt é escopado ao esquema visível da função. (REQ-356) O documento de resultado chaveia cada ramo por alvo: (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Um ramo que esgota seu limite de iteração retorna `query: null`, `result: null`, e uma string `error`. Toda consulta gerada executa sob os direitos do consumidor com a governança de Estágio 2 aplicada — o serviço nunca contorna a governança. (REQ-359)

#### Agrupamento NL com Detalhes de Dimensão (REQ-1405)

Quando uma consulta NL de agrupamento também projeta colunas de uma tabela de dimensão unida — por exemplo, "contagem de consultas por usuário com nome de usuário e email" — o executor deriva caminhos ponto a ponto por campo (`dim_paths`) a partir das colunas de dimensão projetadas no SELECT. Esses caminhos populam o parâmetro `includeNodes=` nas URLs geradas pelos painéis JSON:API e OpenAPI, de forma que esses painéis solicitam os mesmos campos de dimensão unida que os ramos SQL e GraphQL resolveram. Sem isso, `includeNodes=true` retornaria apenas os campos escalares próprios da tabela de agregação base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

No painel gRPC, o `{Type}GroupByRequest` gerado carrega `include_nodes` (bool) e `include` (string repetida de nomes de campo de relacionamento). O `{Type}GroupByRow` retornado inclui um campo `nodes` tipado com as linhas de detalhe de dimensão. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Retorna o SDL GraphQL para o esquema de uma função. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**Cabeçalhos:** `X-Role: <role_id>` (obrigatório)

**Parâmetros de consulta:**

- `domain` — IDs de domínio separados por vírgula. Quando definido, a resposta é filtrada para o(s) domínio(s) nomeado(s) e tabelas alcançáveis a partir deles.

**Resposta:** SDL GraphQL em `text/plain`.

---

### `GET /data/introspection`

Retorna o JSON de introspecção GraphQL, opcionalmente filtrado por domínio. [tool-verified: `provisa/api/data/sdl.py:200`]

**Cabeçalhos:** `X-Provisa-Role: <role_id>` (obrigatório)

**Parâmetros de consulta:** `domain` — IDs de domínio separados por vírgula.

**Resposta:** resultado de introspecção `application/json`.

---

### `GET /data/graph-schema`

Retorna a visão em grafo do esquema da função: rótulos de nó e seus tipos de relacionamento, para clientes Cypher/grafo. Inclui `pk_columns` por rótulo de nó para que os chamadores possam determinar as colunas de chave primária. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Resposta:** `application/json` com `node_labels` (cada um carregando `pk`/`pk_columns`) e `relationship_types`.

Cada tipo de relacionamento carrega também `junction_table_name` e `properties` (REQ-1586). Em uma aresta apoiada em junção, o primeiro nomeia a tabela associativa que ela percorre e o segundo lista as colunas dessa tabela legíveis como `r.attr` e filtráveis em `WHERE`; em uma aresta apoiada em chave estrangeira o nome é `null` e a lista de propriedades fica vazia, que é como um cliente distingue as duas. A própria tabela de junção nunca é um rótulo de nó — ela é a aresta, portanto não tem pílula em um cliente de grafo nem linha em `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Retorna IDs de domínio acessíveis à função requisitante. [tool-verified: `provisa/api/data/sdl.py:116`]

**Cabeçalhos:** `X-Role: <role_id>` (obrigatório)

**Resposta:** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Retorna a string de versão do esquema atual. Combina um nonce por boot com um contador de reconstrução. Clientes usam isso para invalidar caches de esquema após reinicializações do servidor. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Resposta:** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Retorna o arquivo `.proto` auto-gerado para uma função. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Resposta:** esquema protobuf em `text/plain`.

Cada tabela registrada produz uma `message` proto. Relacionamentos produzem campos de mensagem aninhados. Mapeamento de tipos: `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Stream de Server-Sent Events para notificações de mudança em tempo real a partir de uma tabela. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

A entrega de notificações usa um provedor plugável escolhido por tipo de fonte: fontes PostgreSQL usam `LISTEN/NOTIFY` (via asyncpg), fontes MongoDB usam Change Streams (`collection.watch()`), e fontes Kafka usam grupos de consumidores. Cada provedor implementa uma interface comum de observação assíncrona. Filtragem RLS e validação de esquema se aplicam independentemente do provedor. (REQ-258) Fontes WebSocket e RSS também são suportadas. (REQ-338, REQ-342)

**Cabeçalho — `X-Provisa-Sink`:** Defina para um alvo Kafka (ex.: `kafka://broker:9092/topic`) para redirecionar eventos de mudança para um sink Kafka em vez da resposta SSE. O servidor inicia um consumidor sink e retorna `202 Accepted` em vez de um stream aberto. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoints REST de Administração

### Config

#### `GET /admin/config`

Baixa o `provisa.yaml` atual como `application/x-yaml` com um cabeçalho `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Envia um YAML de configuração revisado. O servidor grava um backup `.bak`, salva o novo arquivo, e recarrega todos os esquemas, fontes e views materializadas. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corpo da requisição:** Conteúdo YAML bruto.

**Resposta:**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

Em falha de recarregamento: `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Baixe a **configuração viva atual** — a configuração como o Provisa a escreveria hoje, refletindo cada tabela, relacionamento, domínio, função e regra de RLS criada pela administração que se acumulou desde a inicialização. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

O arquivo em disco pode ficar atrás do estado vivo se mudanças foram feitas pela API de administração sem um envio posterior. Este endpoint fecha essa lacuna: sua saída é o que `PUT /admin/config` precisaria receber para que o arquivo em disco combine com o estado vivo.

Retorna `application/x-yaml` com `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Retorna os dois lados do diff de configuração — `original` (a linha de base da inicialização) e `current` (o estado vivo) — normalizados de forma idêntica, de modo que a comparação mostre apenas mudanças genuínas, não reordenação ou variação de comentários. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Resposta:**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Gera um patch em diff unificado da linha de base para a configuração enviada. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Envie o YAML revisado como corpo da requisição. A resposta é um arquivo `text/x-patch` (`provisa.config.patch`) que `git apply` ou `patch` consegue consumir diretamente — útil para submeter mudanças de configuração feitas pela UI através de um pipeline de CI/CD.

---

### Settings

#### `GET /admin/settings`

Retorna as configurações atuais da plataforma como JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Atualiza as configurações da plataforma em tempo de execução. Todos os campos são opcionais — apenas chaves presentes no corpo são atualizadas. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming`: `domain_prefix`, `convention` — grava no arquivo de configuração e aciona recarregamento de esquema (REQ-253)
- `relationships`: `auto_track_fk` — governa apenas o rastreamento de chaves estrangeiras. Um relacionamento apoiado em junção é declarado no registro da tabela e nunca é inferido, então esta configuração não o alcança. (REQ-1586)
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Resposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modelos de IA

#### `GET /admin/ai-models`

Retorna as atribuições de modelo de IA da organização atuante, o registro de modelos de vetor e o limite de taxa de NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

Chaves de API nunca são devolvidas — `api_keys_set` informa apenas se cada fornecedor tem uma chave configurada. As mudanças entram em vigor na próxima requisição; nenhuma reinicialização é necessária. (REQ-1349)

#### `PUT /admin/ai-models`

Atualiza as atribuições de modelo de IA da organização, o registro de modelos de vetor ou o limite de taxa de NL. Entra em vigor na próxima requisição. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Retorna os nomes de modelo que um fornecedor serve no momento, para o seletor de modelos. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

A lista é lida ao vivo da própria API de listagem de modelos do fornecedor usando a chave configurada da organização — ou a credencial da implantação, quando nenhuma chave da organização está definida. Um modelo lançado depois deste build é selecionável no mesmo dia em que o fornecedor o serve.

Retorna `400` quando o fornecedor não publica uma API de listagem de modelos (nesse caso, digite o nome do modelo diretamente) ou quando nenhuma chave está disponível. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Motor de Federação

#### `GET /admin/federation-engine`

Retorna a seleção atual de motor de federação, sua configuração de conexão e o registro completo de motores selecionáveis. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

Persiste uma seleção de motor de federação. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Corpo da requisição:**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

A seleção é escrita na configuração da plataforma. Ela entra em vigor após a próxima reinicialização do serviço — o motor é escolhido uma vez, na inicialização.

---

### Política de Domínio

#### `POST /admin/domain-policy`

Altera a política de domínio da organização atuante (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Esta é uma operação destrutiva com escopo na organização atuante. Toda fonte, tabela, domínio e relacionamento registrado é expurgado e reconstruído sob a nova política. Use-a ao trocar uma organização de domínios nomeados para plana (ou vice-versa).

**Corpo da requisição:**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` limpa a sobreposição da organização e volta à configuração no nível da implantação. `use_domains: false` exige `default_domain` (o único nome de domínio em que todas as tabelas caem). A reconstrução do catálogo é síncrona; a resposta retorna quando os esquemas estão prontos.

---

### Observabilidade

#### `GET /admin/traces/recent`

Retorna até N spans concluídos recentes do buffer de spans em memória. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Parâmetros de consulta:** `limit` (padrão 50, máximo 200)

**Resposta:** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recarrega a quente um catálogo nomeado no coordenador do motor de federação via sua API REST. Reconecta a conexão interna do Provisa e reexecuta o DDL do OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Parâmetros de consulta:** `catalog` (padrão `"otel"`)

**Resposta:**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Reinicia o container do motor de federação (apenas desenvolvimento de nó único). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Parâmetros de consulta:** `container` (padrão a variável de ambiente `QUERY_ENGINE_CONTAINER`, depois `"trino"`)

---

### Descoberta

#### `POST /admin/discover/relationships`

Aciona a descoberta de relacionamentos. Sempre executa introspecção de FK a partir do motor de federação. (REQ-018) Executa inferência LLM se `ANTHROPIC_API_KEY` estiver definida. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

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

Lista candidatos de relacionamento pendentes. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Aceita um candidato e o registra como relacionamento. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Corpo da requisição (opcional):** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejeita um candidato. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Corpo da requisição:** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Retorna a contagem de candidatos rejeitados. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Exclui todos os candidatos rejeitados. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Crawl de Fonte

#### `POST /admin/sources/crawl`

Percorre uma fonte de dados para introspectar seu esquema e registrar tabelas. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Busca de Tabela na Fonte

#### `GET /admin/sources/{source_id}/tables/search`

Busca tabelas disponíveis (ainda não registradas) em uma fonte por nome. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Perfil de Tabela

#### `POST /admin/tables/{table_id}/profile`

Executa um perfil de coluna em uma tabela registrada — cardinalidade, mín/máx, taxas de nulo. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descrições de Fonte

#### `POST /admin/source-meta/db-description`

Gera descrições assistidas por LLM para as tabelas e colunas de uma fonte. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Armazenamento de Objetos (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Informa o consumo de armazenamento da organização atuante frente à sua cota de plataforma, e se a organização registrou o próprio armazenamento. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Quando a organização registrou seu próprio DSN, as materializações dela vão para lá e deixam de contar contra a cota. O DSN em si nunca é devolvido.

#### `PUT /admin/org-storage`

Registra (ou limpa) o armazenamento de materialização da própria organização. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Corpo da requisição:**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

O DSN é validado contra o motor de federação antes de ser aceito — um DSN inutilizável falha no registro, e não horas depois durante uma atualização. O valor é criptografado em repouso e nunca é devolvido pelo GET.

Envie `storage_url: null` para limpar o armazenamento próprio da organização e devolver suas materializações ao armazenamento da plataforma (e à cota). O runtime da organização é reconstruído na mesma chamada, então o novo armazenamento passa a valer imediatamente. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Criptografia da Organização (REQ-1574)

#### `GET /admin/org-encryption`

Retorna o estado atual da chave da organização: impressão digital, id e procedência. Nunca retorna material de chave. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Quando a organização não definiu chave alguma, retorna `{"configured": false}`. Toda organização começa nesse estado e herda a chave da implantação.

#### `PUT /admin/org-encryption`

Define ou rotaciona a chave de criptografia em repouso da organização. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Corpo da requisição:**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omita `key_b64` para que o Provisa gere uma chave — o caminho mais seguro, já que a chave nunca aparece em uma área de transferência ou log de requisição. Fornecer `key_b64` traz sua própria chave.

A rotação adiciona uma nova entrada ativa ao chaveiro e mantém a antiga, de modo que dados escritos sob a chave anterior seguem legíveis. Rotação não é recriptografia. Não há endpoint de exclusão: aposentar a última chave tornaria ilegível todo payload envolvido por ela. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

O chaveiro vivo é revinculado na mesma chamada, então a próxima escrita criptografada já usa a nova chave.

---

### Importação Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Converte um arquivo de projeto Hasura v2 ou DDN em configuração proposta do Provisa sem gravar nada. [tool-verified: `provisa/api/admin/import_router.py`]

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

Carrega na organização atuante uma configuração previamente pré-visualizada. [tool-verified: `provisa/api/admin/import_router.py`]

**Corpo da requisição:**

```json
{"config_yaml": "<yaml string>"}
```

Usa o mesmo caminho de recarga a quente de `PUT /admin/config`. O catálogo, os esquemas e os pools da organização são reconstruídos antes de a resposta retornar.

---

### Intercâmbio com o Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Exporta o modelo governado da organização como um documento YAML do Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

O documento é derivado do estado vivo a cada requisição — nunca armazenado em cache — portanto não pode ficar desatualizado. Tabelas viram objetos `dataset`, colunas viram objetos `field` e relacionamentos mapeiam para objetos `relationship` do Ossie.

Retorna `text/yaml` com `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Analisa um documento YAML ou JSON do Ossie e retorna registros propostos de tabela e relacionamento. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

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

### Actions (Funções e Webhooks)

Todos os endpoints estão sob o prefixo `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Toda invocação — de GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, e gRPC Provisa — passa por um único executor governado que aplica `writable_by` e governança de forma uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Veja [docs/integrations.md](integrations.md#invocando-comandos-entre-protocolos) para a sintaxe de chamada por protocolo.

#### `GET /admin/actions`

Retorna todas as funções de BD e webhooks rastreados. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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
      "writableBy": [],
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

Cada objeto de webhook carrega um booleano `approved`. Um webhook é aprovado assim que um steward executa sua requisição de criação (REQ-209); webhooks declarados em config são auto-aprovados. Um webhook não aprovado é registrado mas não exposto em nenhuma superfície. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Registra uma função rastreada (comando). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Campos-chave:**

| Campo | Obrigatório | Descrição |
| --- | --- | --- |
| `name` | Sim | Nome de comando único |
| `kind` | Sim | `"query"` → campo Query GraphQL; `"mutation"` → campo Mutation |
| `implKind` | Não | Como o comando executa — ver tabela abaixo (padrão `source_procedure`) |
| `binding` | Não | Detalhes de conexão específicos de `implKind` (objeto JSON) |
| `returnSchema` | Não | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — torna o comando set-returning em toda superfície |
| `arguments` | Não | Definições de argumento `[{name, type}]`; a ordem posicional importa para chamadores SQL e Bolt |
| `visibleTo` | Não | IDs de função que podem chamar o comando |
| `writableBy` | Não | IDs de função autorizados a invocá-lo como mutação |
| `domainId` | Não | Domínio para posicionamento GraphQL e controle de acesso |

**Valores de `implKind`:**

| `implKind` | O que executa | Campos de `binding` |
| --- | --- | --- |
| `source_procedure` | Stored procedure em uma fonte registrada (padrão) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script do lado do servidor | `script` |
| `http` | Chamada HTTP de saída | `url`, `method` |
| `grpc` | Chamada gRPC de saída a um servidor externo | `target`, `method` |
| `python` | Callable Python hospedado pelo Provisa (REQ-885) | `callable` (ex.: `"demo.py_functions:random_dataset"`) |

Os comandos de demonstração `random_python_set` (`implKind: python`) e `random_grpc_set` (`implKind: grpc`) mostram comandos set-returning com `returnSchema` na prática; ambos estão em `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Atualiza uma função rastreada pelo nome. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Exclui uma função rastreada pelo nome. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Registra um webhook rastreado. (REQ-209) Registrar ou atualizar um webhook enfileira uma requisição de aprovação de steward — o webhook se torna ativo em todas as superfícies apenas após um steward aprová-lo. Webhooks declarados em config são auto-aprovados. **Campos do corpo da requisição:** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Atualiza um webhook rastreado pelo nome. Qualquer edição redefine a aprovação para pendente até ser reaprovada. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Exclui um webhook rastreado pelo nome. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Testa uma action (função ou webhook) pelo nome. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Funções

Todos os endpoints estão sob o prefixo `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Método | Caminho | Descrição |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Lista todas as funções |
| `POST` | `/admin/roles/` | Cria uma função |
| `PUT` | `/admin/roles/{role_id}` | Atualiza uma função |
| `DELETE` | `/admin/roles/{role_id}` | Exclui uma função |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Usuários

Todos os endpoints estão sob o prefixo `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

### Organizações

Todos os endpoints estão sob `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

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

### Convites

Todos os endpoints estão sob `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Método | Caminho | Descrição |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Cria um convite |
| `GET` | `/admin/invites/` | Lista convites pendentes |
| `DELETE` | `/admin/invites/{token}` | Revoga um convite |

---

### GraphQL de Administração

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry para todas as operações de administração: CRUD de fontes e tabelas, gerenciamento de relacionamentos, configuração de domínio, regras RLS, controle de cache, convenções de nomenclatura, gerenciamento de tarefas programadas, e compilação de consultas. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Setup

#### `GET /setup/status`

Retorna o status de configuração de primeira execução. Sempre não autenticado. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Completa a configuração de primeira execução. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Health Check

#### `GET /health` ou `HEAD /health`

Retorna `{"status": "ok"}`. Sempre não autenticado. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Respostas de Erro

| Status | Significado |
| --- | --- |
| 400 | Consulta inválida, erro de validação, ou erro de parse SQL |
| 401 | Token de autenticação ausente ou inválido |
| 403 | Capacidades insuficientes; violação de governança |
| 404 | Função, recurso, ou arquivo de configuração não encontrado |
| 422 | Cabeçalho obrigatório ausente (ex.: `X-Role`) |
| 503 | Banco de dados ou fonte não conectado; dependência indisponível |
| 504 | Requisição expirou |

Violações de governança em `POST /data/sql` retornam HTTP 403 com um corpo estruturado: (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

## Endpoint Arrow Flight

Porta `8815`. Transporte colunar Arrow nativo sobre gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Consultas e descoberta de catálogo estão ambas disponíveis na mesma conexão. O pipeline de governança completo (RLS, mascaramento, amostragem) é aplicado a cada consulta. (REQ-130, REQ-143)

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

Quando o proxy Zaychik Flight SQL está disponível (porta 8480), os record batches fazem streaming de ponta a ponta sem materialização completa. (REQ-144) Recorre à materialização via a camada de consulta federada se o Zaychik não estiver disponível. (REQ-146)

---

## Endpoint Protobuf gRPC

Porta `50051` (sobreponha com a variável de ambiente `GRPC_PORT` ou a configuração `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passe a função na chave de metadados gRPC `x-provisa-role`. Se ausente, o servidor aborta com `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Baixe o proto específico da função em `GET /data/proto/{role_id}`. Apenas tabelas e colunas visíveis a essa função aparecem. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Cada tabela produz um RPC de streaming `Query{TypeName}`. RPCs `Insert{TypeName}` existem por simetria de esquema mas abortam com `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` está habilitado para descoberta de serviço sem um proto pré-compilado. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

O servidor gRPC só inicia quando um proto válido pode ser compilado na inicialização. Se a construção do esquema falhar, o servidor gRPC não inicia. (REQ-529)

#### RPCs de Agregação e Agrupamento (REQ-1359, REQ-1361, REQ-1405)

Quando uma tabela tem `enable_aggregates` definido, o proto gerado inclui dois RPCs adicionais além de `Query{TypeName}`:

- **`Query{TypeName}Aggregate`** — retorna escalares de agregação para a tabela (`count`; `sum`, `avg`, `stddev`, `variance` por coluna numérica; `min`, `max` por coluna comparável)
- **`Query{TypeName}GroupBy`** — retorna uma linha por chave de grupo com subcampos de agregação e, opcionalmente, escalares da tabela base e linhas de dimensão unida em um campo `nodes`

Ambos passam pelo mesmo pipeline de agregação do compilador que os campos raiz `{field}_aggregate` e `{field}_group_by` do GraphQL — sem implementação de agregação separada. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Campo `funcs` (REQ-1361).** A mensagem de requisição aceita um campo `funcs` de string repetida. Os valores válidos são `count`, `sum`, `avg`, `stddev`, `variance`, `min`, e `max`. Quando `funcs` é omitido, toda função que o esquema expõe para essa tabela é solicitada. Quando definido, apenas as funções nomeadas aparecem. Se nenhuma das funções nomeadas se aplicar aos tipos de coluna da tabela, a consulta recorre a `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Campos `include_nodes` e `include` (REQ-1405).** Requisições `Query{TypeName}GroupBy` podem definir `include_nodes: true` para incluir colunas escalares da tabela base no campo `nodes` de cada linha. O campo `include` de string repetida nomeia campos de relacionamento muitos-para-um cujas colunas escalares também são aninhadas dentro de `nodes`. Isso corresponde ao comportamento `?includeNodes=` / `?include=` do JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Driver JDBC

O driver JDBC do Provisa (`provisa-jdbc-0.1.0.jar`) expõe o catálogo semântico para ferramentas de BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de conexão:** `jdbc:provisa://host:port` (REQ-131)

Domínios mapeiam para esquemas JDBC. (REQ-127) Tabelas usam seus aliases registrados. Colunas usam aliases e expõem descrições como `REMARKS`. (REQ-128) Métodos de metadados padrão (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) expõem relacionamentos semânticos como metadados PK/FK.

**Suporte SQL:** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

O driver solicita redirecionamento Arrow IPC por padrão. Resultados fazem streaming batch-by-batch via `ArrowStreamReader`, limitados a um record batch em memória. (REQ-293)

---

## Formato do Argumento `orderBy`

O argumento `order_by` usa objetos `{column: direction}` com um enum de direção de 6 valores: (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Direções suportadas: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Subscriptions

Subscriptions SSE estão disponíveis em `GET /data/subscribe/{table}`. (REQ-219, REQ-258) A entrega de notificações usa um provedor plugável selecionado por tipo de fonte: fontes PostgreSQL usam `LISTEN/NOTIFY`, fontes MongoDB usam Change Streams, e fontes Kafka usam grupos de consumidores. Filtragem RLS e validação de esquema se aplicam independentemente do provedor. Fontes WebSocket e RSS também são suportadas via o mesmo endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Glossário de Negócios (REQ-1387)

O glossário de negócios mapeia nomes de campo físicos — como existem nos bancos de dados de origem — para um vocabulário humano compartilhado. Toda coluna registrada na camada semântica recebe um termo automaticamente. Nenhuma entrada manual é necessária para popular o glossário; curadores adicionam definições, relacionamentos e especialistas sobre o que o sistema deriva.

### Como os Termos São Derivados

Quando o Provisa registra ou atualiza as colunas de uma tabela, `normalize_term` (`provisa/core/glossary.py`) roda em cada nome de coluna e produz uma frase canônica. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

A normalização aplica cinco regras em sequência:

1. Divide em limites de camelCase e caracteres separadores (`_`, `-`, `.`, `/`, espaço em branco).
2. Converte o resultado para minúsculas.
3. Expande uma tabela de abreviações fixa (ex.: `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Remove um **token proxy** final (`identifier`, `code`, `index`, ou `reference`) — uma coluna nomeada por sua chave ou código está apontando para o conceito subjacente através de um valor substituto, então o termo deve ser o próprio conceito. O último token restante nunca é removido.
5. Qualifica uma **frase genérica demais** com o conceito da tabela. Quando a frase normalizada completa é uma palavra de atributo simples (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, e similares), o termo se torna `<conceito da tabela> <frase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Um único termo `name` compartilhado entre tabelas não relacionadas mesclaria significados distintos; a qualificação conecta cada coluna ao seu conceito envolvente em vez disso. O conceito da tabela é o nome de negócio da tabela, normalizado com um substantivo núcleo no singular (`order_lines` → `order line`).

Pseudocolunas de filtro nativo (prefixadas com `_nf_`, ou qualquer coluna que carregue `native_filter_type`) são mecanismo de parâmetro de consulta, não campos de negócio, e não derivam termos.

Como `id`, `key`, `pk`, e `sk` todos se expandem para `identifier` antes da verificação de proxy, três nomes de coluna fisicamente diferentes recaem exatamente no mesmo termo:

| Nome físico | Após normalização |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Os três primeiros colapsam em um termo. `transaction amount` mantém ambos os tokens porque `amount` não é um proxy. Uma coluna `id` simples — sem tokens precedentes — não pode ser removida; ela normaliza para `identifier` para que o termo não fique vazio. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Ciclo de Vida

Termos são **derivados da associação à camada semântica**, não criados sob demanda por usuários. O repositório de tabela é o único caminho de escrita: `sync_table_refs` roda dentro de todo upsert de conjunto de colunas, e `sweep_refless_terms` roda após qualquer caminho de exclusão. [tool-verified: `provisa/core/repositories/glossary.py`]

**Quando uma coluna é adicionada:** o Provisa busca o termo normalizado pelo nome. Se ele já existir, a coluna recebe uma referência a ele (e se o termo estava depreciado, ele é revivido — `deprecated` é definido de volta para `False`). Se nenhum termo existir ainda, um é criado.

**Quando uma coluna sai** (mudança de esquema ou remoção de tabela): sua referência é excluída e o termo é **resolvido** sob uma regra de remover-ou-depreciar. Um termo enraizado sem referências restantes é removido completamente — junto com suas arestas e atribuições de especialista — a menos que removê-lo deixasse um termo abstrato desconectado de todos os termos enraizados (sem caminho através do grafo de termos). Nesse caso, o termo é **depreciado** (marcado `deprecated=True`) em vez de excluído, para que a âncora de grafo do termo abstrato sobreviva.

Termos abstratos nunca são removidos automaticamente; eles existem fora do ciclo de vida físico e são excluídos apenas explicitamente via a API de administração.

**Revival:** se o nome normalizado de um termo depreciado reaparece (uma coluna é reregistrada), o termo é desmarcado e suas referências voltam a se acumular.

### Endpoints de Curadoria

Todos os endpoints estão sob `/admin/glossary`. Eles exigem acesso `org_admin` e uma organização configurada. Toda mutação aciona uma publicação de metadados. [tool-verified: `provisa/api/admin/glossary_router.py`]

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

Valores válidos de `rel_type`: `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Corpo de `POST /admin/glossary/terms/{term_id}/experts`:**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valores válidos de `kind`: `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

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

Definir `export_excluded` como `true` retém o termo de todos os snapshots de exportação de metadados, independentemente de suas referências físicas ou status abstrato. Defini-lo de volta para `false` restaura o termo ao snapshot na próxima publicação. Dados de curadoria (definição, arestas, especialistas) não são afetados. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curadoria Assistida por IA

O modelo de IA configurado da organização pode rascunhar definições e propor arestas de relacionamento em todo o glossário em uma única operação. Ambas as ações em lote exigem acesso `org_admin` e uma organização configurada.

**`POST /admin/glossary/definitions/generate`**

Itera por todo termo no glossário, pula qualquer um que já tenha uma definição, e chama o modelo de IA da organização para rascunhar uma para cada termo restante. O rascunho é persistido imediatamente — diferente do endpoint de rascunho por termo (`POST /admin/glossary/terms/{term_id}/definition/generate`), não há etapa de edição. Definições de autoria humana nunca são sobrescritas: a proteção é `if summary["definition"]: continue` antes de qualquer chamada de modelo. Uma notificação de publicação cobre o lote inteiro. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Resposta:

```json
{"generated": 12}
```

`generated` é a contagem de termos que receberam uma nova definição. É zero quando todo termo já tem uma.

**`POST /admin/glossary/relationships/generate`**

Envia a lista completa de termos ao modelo de IA da organização com um prompt que especifica os dez tipos de aresta permitidos (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) e pede apenas propostas confiáveis. O modelo responde com um array JSON; cada entrada é validada antes de qualquer escrita: nomes de termo desconhecidos, auto-arestas, e tipos de aresta fora do enum fechado são descartados silenciosamente. Propostas válidas são inseridas/atualizadas (upsert) de forma idempotente — reexecutar a ação não duplica arestas. Uma notificação de publicação cobre o lote. O endpoint retorna `{"added": 0}` imediatamente quando o glossário contém menos de dois termos não depreciados. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Resposta:

```json
{"added": 5}
```

`added` é a contagem de arestas escritas. Uma aresta que já existia ainda conta — o upsert é bem-sucedido, mas os dados da aresta não mudam.

### Ferramenta MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Busca nomes e definições de termos com correspondência de substring sem distinção de maiúsculas/minúsculas, até `limit` resultados. Cada resultado é o detalhe completo do termo: `name`, `definition`, `is_abstract`, `deprecated`, referências físicas (com `source_id`, `schema_name`, `table_name`, `column_name`), arestas tipadas, e atribuições de especialista. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Use `search_terms` antes de escrever SQL para encontrar todo campo físico que representa um conceito por nome. Por exemplo, buscar `"order date"` retorna o termo e todas as colunas `order_dt`, `orderDate`, `ORDER_DATE` em toda tabela registrada.

### Exportação de Metadados

O grafo de termos do glossário é incluído em todo `MetadataSnapshot` construído por `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

A exportação aplica os mesmos filtros que o restante do snapshot:

- Um termo marcado `export_excluded` é retido completamente — independentemente de suas referências físicas, status abstrato, ou se o catálogo da organização está configurado. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
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

A propriedade é o vínculo, não o nome: o id de fornecedor de cada termo publicado é capturado em `catalog_bindings` sob o URN do termo (`provisa://<org>/terms/<name>`), e o Provisa modifica ou exclui um item de glossário do lado do fornecedor apenas quando detém esse vínculo (ou o item vive no contêiner de propriedade do Provisa que ele criou). Um item de glossário sem vínculo Provisa se originou no sistema externo e nunca é tocado; atualizações fazem read-merge para que campos adicionados por stewards nos próprios termos do Provisa sobrevivam; nada é excluído quando um termo sai do snapshot. Atribuições de termo-para-ativo feitas por stewards permanecem de propriedade externa — nenhum adaptador escreve atribuições de termo-para-ativo (a publicação de atribuições de autoria do Provisa é um follow-on explícito). Especificamente no Collibra, a segurança sob a semântica REPLACE da Import API repousa na contenção: o payload menciona apenas ativos dentro do domínio de glossário Provisa e instâncias de relação apenas entre termos Provisa, de forma que glossários de stewards e suas relações nunca são alcançáveis. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
