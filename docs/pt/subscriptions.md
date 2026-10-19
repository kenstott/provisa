# Subscriptions SSE {: #sse-subscriptions }

O Provisa suporta push em tempo real via Server-Sent Events (SSE). Clientes recebem um stream de eventos de mudança sem polling.

## Fontes {: #sources }

Subscriptions têm como alvo uma **tabela registrada**:

| Fonte | Valores de `strategy` disponíveis |
| -------- | ------------------------- |
| Tabela (PostgreSQL) | `native` (LISTEN/NOTIFY), `poll` |
| Tabela (RDBMS não-PG com um bloco `cdc` na fonte) | `debezium`, `kafka`, `poll` |
| Tabela (view federada / qualquer outra fonte) | somente `poll` |

### Auto-instalação de trigger PostgreSQL {: #postgresql-trigger-auto-installation }

O Provisa automaticamente instala triggers `AFTER INSERT OR UPDATE OR DELETE` em todas as tabelas PostgreSQL **pré-aprovadas** na inicialização. Esses triggers chamam `pg_notify('provisa_{table}', ...)` para que DML bruto (não apenas mutações do Provisa) seja captado pelas subscriptions.

Se a instalação do trigger falhar (ex.: privilégio insuficiente — a função do banco de dados deve possuir a tabela), o Provisa recorre ao polling por marca d'água para aquela tabela, desde que um `watermark_column` esteja configurado. Um aviso é registrado no log.

### Subscriptions de view entre fontes de dados {: #cross-datasource-view-subscriptions }

Para views que unem múltiplas fontes de dados via o motor de federação, adicione um `watermark_column` ao registro da tabela. A coluna deve existir no SQL da view (não precisa aparecer no esquema GraphQL):

```sql
-- Example: federated view with derived watermark
CREATE OR REPLACE VIEW orders_with_segments AS
SELECT o.*, s.name AS segment_name,
       GREATEST(o.updated_at, s.updated_at) AS _watermark
FROM postgresql.public.orders o
JOIN mysql.crm.customer_segments s ON o.customer_id = s.customer_id;
```

Registre com `watermark_column: _watermark`. O Provisa faz polling usando `WHERE _watermark > <last_seen>`.

### Subscriptions de relacionamento aninhado {: #nested-relationship-subscriptions }

Quando o campo de subscription seleciona campos de tabelas unidas (via relacionamentos registrados), o Provisa observa **todas** as tabelas físicas envolvidas simultaneamente. Uma mudança em qualquer tabela unida dispara novamente a consulta de subscription.

## Endpoint

Inscreva-se em uma tabela:

```http
GET /data/subscribe/{table}
Accept: text/event-stream
```

A conexão permanece aberta e emite um evento JSON por mudança:

```text
data: {"event":"insert","table":"orders","row":{"id":43,"amount":55.00,"region":"east"}}

data: {"event":"update","table":"orders","row":{"id":42,"amount":199.00,"region":"west"}}
```

## Modos de Entrega {: #delivery-modes }

A entrega é selecionada por `live.strategy` na config da tabela:

| `strategy` | Mecanismo | Disponível para | Requer |
| ------------ | ----------- | --------------- | --------- |
| `native` | PostgreSQL `LISTEN`/`NOTIFY`, MongoDB Change Streams | PG, MongoDB | Nada extra |
| `debezium` | Tópico Kafka do conector Debezium | Tabelas RDBMS não-PG | Bloco `cdc` em nível de fonte (Debezium + Kafka) |
| `kafka` | Tópico delta Kafka arbitrário | Qualquer tabela alimentada por Kafka | Bloco `cdc` em nível de fonte |
| `poll` | Polling baseado em marca d'água | Qualquer tabela com marca d'água | `watermark_column` |

### LISTEN/NOTIFY

O Provisa emite `LISTEN <channel>` em uma conexão PG persistente. Mutações do Provisa disparam `NOTIFY` automaticamente. Escritores externos devem chamar `NOTIFY <channel>, '<payload>'` após escritas. Nenhuma infraestrutura adicional é exigida.

### Polling

O Provisa reexecuta a consulta de origem periodicamente, selecionando apenas linhas onde `watermark_column > last_watermark`. Diffs são emitidos como eventos SSE. O poll não consegue ver hard deletes — uma linha removida não deixa marca d'água avançando. Para tornar uma exclusão visível, use um soft delete (ex.: defina uma flag `deleted_at`) que avance a coluna de marca d'água; a exclusão então chega como um evento de atualização carregando o marcador de soft-delete.

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

### CDC Debezium {: #debezium-cdc }

Requer um conector Debezium em execução escrevendo no Kafka. O Provisa consome o tópico Kafka e encaminha eventos de mudança para os clientes SSE conectados.

O transporte CDC é configurado uma vez por fonte em um bloco `cdc`; tópicos são derivados como `{topic_prefix}.{schema}.{table}` e nunca repetidos por tabela. Cada tabela então seleciona `strategy: debezium`:

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

#### Codificações de mensagens {: #message-encodings }

Um tópico Debezium é lido em uma de duas codificações, e cada mensagem diz qual é.

| Codificação | O que o tópico traz | O que a fonte precisa |
|---|---|---|
| JSON | O envelope do conversor JSON do Debezium, com ou sem o invólucro `schema`/`payload` | Nada além do bloco `cdc` |
| Avro | O formato de transmissão do schema registry da Confluent: um byte zero, um id de esquema de quatro bytes, um dado Avro | `schema_registry_url`, e como se autenticar nele |

As mensagens Avro são decodificadas com o esquema que o seu id nomeia, buscado no registry uma vez e guardado. Um tópico cujo esquema evoluiu é lido corretamente, porque cada mensagem nomeia o esquema que a escreveu. Tópicos Protobuf e JSON Schema não são lidos.

Uma mensagem Avro nunca é lida como JSON. Cada forma de não conseguir ler uma é recusada pelo nome:

| Situação | Recusa |
|---|---|
| Mensagem Avro, a fonte não nomeia nenhum registry | `subscribe.avro_topic_without_registry`, com o tópico |
| Registry fora do ar ou sem responder a tempo | `subscribe.schema_registry_unreachable` |
| O registry rejeita as credenciais da fonte | `subscribe.schema_registry_refused_credentials` |
| Id de esquema que o registry não tem | `subscribe.schema_id_unknown`, com o id |
| O id nomeia um esquema Protobuf ou JSON | `subscribe.schema_not_avro` |

Um registry fora do ar ou que rejeita as credenciais é recusado quando a assinatura começa, antes de o fluxo abrir. Uma consulta ao registry espera no máximo 5 segundos para conectar e 10 segundos por uma resposta, e menos quando resta menos tempo à requisição.

#### Autenticação do schema registry {: #schema-registry-authentication }

`schema_registry_auth` nomeia um método; os campos de que esse método precisa são verificados quando a fonte é salva.

| `schema_registry_auth` | Campos obrigatórios | Enviado ao registry |
|---|---|---|
| `none` (padrão) | — | Nenhuma credencial |
| `basic` | `schema_registry_username`, `schema_registry_password` | Autenticação HTTP basic |
| `bearer` | `schema_registry_token` | `Authorization: Bearer <token>` (um token estático) |
| `mtls` | `schema_registry_client_cert`, `schema_registry_client_key` | O certificado do cliente, no handshake TLS |

`schema_registry_ca` é opcional com qualquer método: um pacote de CA em que confiar quando o certificado do registry não é assinado por uma autoridade pública.

A senha e o token seguem o contrato de segredos. Um valor digitado no formulário da fonte é guardado no cofre da organização e a fonte mantém uma referência `${secret:NAME}`; uma referência escrita em um arquivo de configuração (`${env:...}`, `${secret:...}`) é mantida como foi escrita. O certificado, a chave e o pacote de CA são arquivos que o processo do servidor do Provisa abre, nomeados por caminho absoluto.

```yaml
sources:
  - id: sales-mysql
    cdc:
      bootstrap_servers: kafka:9092
      topic_prefix: debezium
      schema_registry_url: https://schema-registry:8081
      schema_registry_auth: basic
      schema_registry_username: provisa
      schema_registry_password: ${secret:registry_password}
      schema_registry_ca: /etc/provisa/registry-ca.pem
```

## Quando um fluxo termina com uma falha {: #when-a-stream-ends-on-a-failure }

Um fluxo que abriu já respondeu `200`, então uma falha depois disso não pode ser um status HTTP. O assinante recebe um último quadro dizendo por quê, e então o fluxo fecha. Um fluxo nunca fecha por uma falha sem esse quadro.

O quadro traz aquilo com que a mesma falha seria respondida em uma requisição comum:

| Falha | Corpo |
|---|---|
| Uma recusa com código (a de um schema registry, a de uma região, uma recusa de governança) | `detail`, `code`, `params` |
| O prazo da requisição | `detail: "Request timed out"` |
| Qualquer coisa inesperada | `detail: "Internal server error"`, `type` (o nome do tipo da exceção) |

Todo corpo também traz `subscription_id`. Em uma falha inesperada, a mensagem e o traceback ficam no log do servidor sob esse id, não no quadro.

A assinatura de tabela (`/data/subscribe/{table}`) envia o corpo como os dados de um evento chamado `stream_error`:

```text
event: stream_error
data: {"detail": "Topic 'debezium.app.orders' carries Avro messages, and its source names no schema registry to read them with: set the source's schema registry URL", "code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}
```

O evento não se chama `error`, porque um `EventSource` dispara seu próprio evento `error` quando uma conexão cai e um ouvinte não conseguiria distinguir os dois. Escute-o pelo nome:

```javascript
source.addEventListener("stream_error", (e) => {
  const { detail, code, params } = JSON.parse(e.data);
  source.close();
});
```

Uma assinatura GraphQL mantém a forma de resposta do GraphQL, com o código e os params em `extensions`:

```json
{"errors": [{"message": "…", "extensions": {"code": "subscribe.avro_topic_without_registry", "params": {"topic": "debezium.app.orders"}, "subscription_id": "5f0c2a9d41b7e386"}}]}
```

Um caso não encerra uma assinatura GraphQL: quando a releitura após uma única mudança falha, esse quadro é enviado na mesma forma e o fluxo segue para a próxima mudança. Uma falha da leitura inicial, do próprio feed de mudanças ou do seu início é o último quadro.

## Redirecionamento de Sink Kafka {: #kafka-sink-redirect }

Qualquer subscription GraphQL pode ser redirecionada para um tópico Kafka em vez de fazer streaming de volta ao cliente. Adicione o cabeçalho `X-Provisa-Sink` à requisição de subscription:

```yaml
POST /data/graphql
Authorization: Bearer <token>
Content-Type: application/json
X-Provisa-Sink: kafka://broker:9092/my-topic
```

O servidor responde `202 Accepted` imediatamente e inicia uma tarefa em segundo plano que:

1. Observa mudanças na tabela usando a mesma resolução de provedor que o SSE (LISTEN/NOTIFY → poll asyncpg → poll federado)
2. Reexecuta a consulta equivalente a cada mudança
3. Publica o resultado como uma mensagem JSON no tópico Kafka nomeado

O sink roda pela vida útil do processo do servidor. Reinicie o servidor para pará-lo.

**Formato de URI:** `kafka://[broker:port]/topic`

- Se `broker:port` for omitido, a variável de ambiente `KAFKA_BOOTSTRAP_SERVERS` é usada (padrão: `localhost:9092`)
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

### Sink Kafka como Segunda Saída em Nível de Config {: #kafka-sink-as-a-config-level-second-output }

Uma subscription de tabela baseada em poll pode simultaneamente publicar em um tópico Kafka via `provisa.yaml`. A subscription SSE e o sink Kafka são ambos saídas do mesmo Live Query Engine. Cada saída rastreia sua marca d'água independentemente.

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
          role: analyst           # required: the rows are published governed as this role
```

Uma saída Kafka publica as linhas da tabela governadas como a função que ela nomeia: suas regras de linha, a visibilidade das colunas e as máscaras se aplicam como em uma leitura dessa função. Uma saída sem `role` é recusada quando a configuração é carregada.

## Segurança {: #security }

Todos os modos de subscription aplicam o mesmo pipeline de segurança das consultas regulares:

- Filtros RLS são aplicados a cada linha emitida
- Colunas mascaradas aparecem mascaradas nos eventos
- A autorização de função é verificada no momento da conexão

## Exemplo de Cliente {: #client-example }

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
