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
  "role": "admin",
  "discovery_mode": false
}
```

A flag `discovery_mode` amplia a verificação de visibilidade de tabela para incluir todas as tabelas de todos os contextos. Apenas para ferramentas internas. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

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

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simples auto-gerado para cada tabela registrada. A query string mapeia para argumentos GraphQL e a requisição compila e executa através do mesmo pipeline (RLS, mascaramento, roteamento) que o GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Parâmetros de consulta:**

- `limit` — máximo de linhas (≥ 1)
- `offset` — pular linhas (≥ 0)
- `fields` — nomes de coluna separados por vírgula (padrão: todos os campos escalares)
- `filter` — array JSON de objetos de filtro `{"field", "comparator", "value"}`
- `orderBy` — array JSON de objetos de ordenação `{"field", "direction"}`

A função autenticada é exigida; requisições não autenticadas retornam `401`. Uma especificação OpenAPI para essas rotas é servida em `GET /data/rest/openapi.json` com Swagger UI em `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint compatível com [JSON:API](https://jsonapi.org), auto-gerado para cada tabela registrada. Mesmo RLS, mascaramento e roteamento que o GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**Cabeçalho `Accept`:** deve incluir `application/vnd.api+json` (o media type JSON:API) ou a requisição retorna `406`.

**Parâmetros de consulta:**

- `fields[<type>]` — sparse fieldsets, ex.: `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — ex.: `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — separado por vírgula, prefixo `-` para descendente, ex.: `?sort=-created_at,amount`
- `page[number]` / `page[size]` — paginação

Respostas são objetos de recurso com `type`/`id`/`attributes`. Erros seguem o formato de objeto de erro do JSON:API.

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
- `relationships`: `auto_track_fk`
- `otel`: `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Resposta:**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

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
