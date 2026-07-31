# Subscriptions SSE

O Provisa suporta push em tempo real via Server-Sent Events (SSE). Clientes recebem um stream de eventos de mudança sem polling. (REQ-258)

## Fontes

Subscriptions têm como alvo uma **tabela registrada**:

| Fonte | Valores de `strategy` disponíveis |
| -------- | ------------------------- |
| Tabela (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabela (RDBMS não-PG com um bloco `cdc` na fonte) | `debezium`, `kafka`, `poll` |
| Tabela (view federada / qualquer outra fonte) | somente `poll` |

### Auto-instalação de trigger PostgreSQL

O Provisa automaticamente instala triggers `AFTER INSERT OR UPDATE OR DELETE` em todas as tabelas PostgreSQL **pré-aprovadas** na inicialização. (REQ-565) Esses triggers chamam `pg_notify('provisa_{table}', ...)` para que DML bruto (não apenas mutações do Provisa) seja captado pelas subscriptions. (REQ-565)

Se a instalação do trigger falhar (ex.: privilégio insuficiente — a função do banco de dados deve possuir a tabela), o Provisa recorre ao polling por marca d'água para aquela tabela, desde que um `watermark_column` esteja configurado. (REQ-566) Um aviso é registrado no log. (REQ-566)

### Subscriptions de view entre fontes de dados

Para views que unem múltiplas fontes de dados via o motor de federação, adicione um `watermark_column` ao registro da tabela. (REQ-260, REQ-283) A coluna deve existir no SQL da view (não precisa aparecer no esquema GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Registre com `watermark_column: _watermark`. O Provisa faz polling usando `WHERE _watermark > <last_seen>`. (REQ-260)

### Subscriptions de relacionamento aninhado

Quando o campo de subscription seleciona campos de tabelas unidas (via relacionamentos registrados), o Provisa observa **todas** as tabelas físicas envolvidas simultaneamente. (REQ-567) Uma mudança em qualquer tabela unida dispara novamente a consulta de subscription. (REQ-567)

## Endpoint

Inscreva-se em uma tabela:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

A conexão permanece aberta e emite um evento JSON por mudança: (REQ-258, REQ-568)

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modos de Entrega

A entrega é selecionada por `live.strategy` na config da tabela: (REQ-813, REQ-814)

| `strategy` | Mecanismo | Disponível para | Requer |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`, MongoDB Change Streams | PG, MongoDB | Nada extra |
| `debezium` | Tópico Kafka do conector Debezium | Tabelas RDBMS não-PG | Bloco `cdc` em nível de fonte (Debezium + Kafka) |
| `kafka` | Tópico delta Kafka arbitrário | Qualquer tabela alimentada por Kafka | Bloco `cdc` em nível de fonte |
| `poll` | Polling baseado em marca d'água | Qualquer tabela com marca d'água | `watermark_column` |

### LISTEN/NOTIFY

O Provisa emite `LISTEN <channel>` em uma conexão PG persistente. (REQ-258) Mutações do Provisa disparam `NOTIFY` automaticamente. (REQ-565) Escritores externos devem chamar `NOTIFY <channel>, '<payload>'` após escritas. Nenhuma infraestrutura adicional é exigida.

### Polling

O Provisa reexecuta a consulta de origem periodicamente, selecionando apenas linhas onde `watermark_column > last_watermark`. (REQ-260) Diffs são emitidos como eventos SSE. O poll não consegue ver hard deletes — uma linha removida não deixa marca d'água avançando. Para tornar uma exclusão visível, use um soft delete (ex.: defina uma flag `deleted_at`) que avance a coluna de marca d'água; a exclusão então chega como um evento de atualização carregando o marcador de soft-delete. (REQ-260)

Config de poll de tabela (em `provisa.yaml`):

```yaml
tables:
  - id: federated_orders
    source_id: federated-source
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
```

### CDC Debezium

Requer um conector Debezium em execução escrevendo no Kafka. (REQ-261) O Provisa consome o tópico Kafka e encaminha eventos de mudança para os clientes SSE conectados. (REQ-261)

O transporte CDC é configurado uma vez por fonte em um bloco `cdc`; tópicos são derivados como `{topic_prefix}.{schema}.{table}` e nunca repetidos por tabela. (REQ-824) Cada tabela então seleciona `strategy: debezium`:

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      # schema_registry_url: http://schema-registry:8081   # set for Avro; omit for JSON
    tables:
      - id: orders
        live:
          strategy: debezium
```

## Redirecionamento de Sink Kafka

Qualquer subscription GraphQL pode ser redirecionada para um tópico Kafka em vez de fazer streaming de volta ao cliente. (REQ-812) Adicione o cabeçalho `X-Provisa-Sink` à requisição de subscription:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

O servidor responde `202 Accepted` imediatamente e inicia uma tarefa em segundo plano que: (REQ-812)

1. Observa mudanças na tabela usando a mesma resolução de provedor que o SSE (LISTEN/NOTIFY → poll asyncpg → poll federado)
2. Reexecuta a consulta equivalente a cada mudança
3. Publica o resultado como uma mensagem JSON no tópico Kafka nomeado

O sink roda pela vida útil do processo do servidor. (REQ-812) Reinicie o servidor para pará-lo (registro persistente de sink via API de administração está planejado).

**Formato de URI:** `kafka://[broker:port]/topic`

- Se `broker:port` for omitido, a variável de ambiente `KAFKA_BOOTSTRAP_SERVERS` é usada (padrão: `localhost:9092`) (REQ-812)
- `topic` é obrigatório

**Exemplo (curl):**

```bash
curl -X POST http://localhost:8000/data/graphql \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Provisa-Sink: kafka://kafka:9092/orders-live" \
  -d '{"query": "subscription { orders { id status amount } }"}'
# → 202 {"status":"streaming","sink":"kafka://kafka:9092/orders-live","table":"orders"}
```

### Sink Kafka como Segunda Saída em Nível de Config

Uma subscription de tabela baseada em poll pode simultaneamente publicar em um tópico Kafka via `provisa.yaml`. (REQ-282, REQ-286) A subscription SSE e o sink Kafka são ambos saídas do mesmo Live Query Engine. (REQ-282) Cada saída rastreia sua marca d'água independentemente. (REQ-286)

```yaml
tables:
  - id: active-orders
    live:
      strategy: poll
      watermark_column: updated_at
      poll_interval: 30
      outputs:
        - type: sse
        - type: kafka
          topic: provisa.active-orders
          bootstrap_servers: kafka:9092
          key_column: id
```

Veja [Sinks Kafka](sources.md) para a referência completa de configuração de sink.

## Segurança

Todos os modos de subscription aplicam o mesmo pipeline de segurança das consultas regulares: (REQ-258, REQ-038)

- Filtros RLS são aplicados a cada linha emitida (REQ-040)
- Colunas mascaradas aparecem mascaradas nos eventos (REQ-040)
- A autorização de função é verificada no momento da conexão (REQ-258)

## Exemplo de Cliente

```javascript
// Table subscription (LISTEN/NOTIFY)
const source = new EventSource('/data/subscribe/orders', {
  headers: { 'Authorization': 'Bearer <token>' }
});

source.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log(event.event, event.row);
};
```
