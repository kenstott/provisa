# Commands

Um comando é uma função registrada e governada que coloca a computação externa sob a governança, a auditoria e a linhagem do Provisa. O motor de federação trata SQL nativamente. Um comando cobre a computação que SQL não consegue expressar, como um microsserviço de enriquecimento, um modelo Python, um script shell ou uma stored procedure nativa do banco. Você o registra uma vez, e toda superfície de cliente (GraphQL, SQL via pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher) pode invocá-lo com governança idêntica.

Os comandos são uma das formas de escrever lógica de negócio no Provisa. Veja [Lógica de negócio no Provisa](business-logic.md).

Um comando é um **RPC governado**. Suas entradas e saídas são declaradas, tipadas, validadas, rastreadas e ligadas à linhagem. Um passo de ETL improvisado, uma chamada curl sem governança ou um subprocesso não têm nada disso.

## Tipos de implementação {: #implementation-kinds }

Seis valores de `impl_kind` são suportados:

| `impl_kind` | Transporte |
| --- | --- |
| `source_procedure` | Procedimento armazenado nativo em uma fonte registrada |
| `source_operation` | Uma operação de escrita de uma fonte OpenAPI, GraphQL remota ou gRPC, repassada como está (veja [Operação de escrita de uma fonte remota](#a-remote-sources-write-operation)) |
| `script` | Subprocesso local alimentado com JSON no stdin, lê JSON do stdout |
| `http` | Endpoint HTTP/S; corpo de requisição JSON, resposta JSON |
| `grpc` | gRPC unário; ponte JSON sem proto |
| `python` | Callable Python em processo (`module:attr`) |

`script` e `python` são código de host: rodam no próprio host do servidor, e uma implantação só os permite quando seu operador liga o código de host (veja [Código de host](#host-code)).

O endereçamento (o `name` no catálogo e o `function_name`) é desacoplado do `binding` (transporte e
localização). Troque o binding e a governança, o lineage e os contratos com quem chama o command permanecem
inalterados.

## Tipos de argumento {: #argument-kinds }

Cada argumento declara um `arg_kind`:

| `arg_kind` | Comportamento |
| --- | --- |
| `column_value` | Escalar; passado diretamente no payload da requisição |
| `table_ref` | Preguiçoso; o Provisa passa a referência da relação como está; o serviço busca os dados |
| `result_set` | Ansioso; o Provisa materializa a relação referenciada e envia suas linhas |

Commands `http` e `grpc` **devem** declarar ao menos um argumento `table_ref` ou `result_set`.
Um command externo recebendo apenas argumentos escalares seria invocado uma vez por linha, o que derrota
o batching. O dispatcher rejeita esta configuração no momento da chamada (422).

Um command que retorna um conjunto (declarado via `output_columns` e `return_schema`) é uma
função com valor de tabela. Use-o em uma cláusula `FROM` ou em um `JOIN`.

## O contrato de conjunto de dados {: #the-dataset-contract }

Cada argumento `table_ref` ou `result_set` pode declarar um **contrato de colunas de entrada**: uma lista ordenada
e tipada em IR de colunas em `FunctionArgument.columns`. O próprio command declara um
**contrato de colunas de saída** em `Function.output_columns`.

Ambos os contratos são validados de forma fail-loud em toda invocação:

- **Entrada (somente result_set):** após a materialização, o Provisa valida as linhas contra as
  colunas declaradas. Campos extras, campos faltantes e tipos errados levantam todos HTTP 422.

- **Saída:** as linhas retornadas pelo command são validadas contra `output_columns` antes de
  chegarem a quem chamou.
- **Projeção estreita:** quando um contrato de entrada é declarado, a consulta de materialização projeta
  **somente essas colunas** (`SELECT "id", "region" FROM ...`) em vez de `SELECT *`.

### O vocabulário de tipos IR {: #the-ir-type-vocabulary }

Os tipos de coluna dos contratos usam os nomes de tipo IR canônicos:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Aliases comuns resolvem automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.).

`return_schema` é a **projeção GraphQL** de `output_columns`. A fonte da verdade é `output_columns`, então declare-o para validação e linhagem, e acrescente `return_schema` para gerar os tipos GraphQL.

## Escrevendo um command {: #authoring-a-command }

### Arquivo de configuração {: #config-file }

Este exemplo é um comando `python`, então ele só carrega onde o código de host é permitido (veja [Código de host](#host-code)).

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

A variante gRPC (`enrich_grpc_set`) segue o mesmo padrão mas especifica `impl_kind: grpc`
e um `binding` com as chaves `target` e `method` em vez de `callable`:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

### UI de administração {: #admin-ui }

O formulário de command em **Configurações → Commands** inclui um editor de colunas de entrada por conjunto de dados (uma linha
por coluna declarada, com um seletor de tipo IR) e um editor de colunas de saída. Salve o formulário para
registrar ou atualizar o command sem recarregar a configuração.

## Composição inline {: #inline-composition }

Comandos podem aparecer **dentro** de uma instrução SQL maior, onde você os junta, usa em subconsultas ou projeta; `SELECT * FROM fn(args)` é só a forma mais simples. A exceção é a operação de escrita de uma fonte remota, que é chamada sozinha (veja [Por que não pode ser composta](#why-it-cannot-be-composed)).

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Antes de a governança, a validação ou o roteamento rodarem, o pipeline detecta chamadas de commands registrados,
executa cada uma através do executor governado compartilhado (de modo que o contrato de I/O e o modelo de identidade se aplicam
exatamente como em uma chamada direta) e reescreve o ponto de chamada como uma relação local tipada.

A substituição se adapta ao tamanho: até 1.000 linhas o resultado é embutido como uma lista `VALUES` tipada;
acima desse limite ele é registrado como uma relação local nomeada no motor.

Uma declaração localizada é roteada normalmente. Consultas de fonte única permanecem na fonte; somente consultas
genuinamente entre fontes vão para o motor de federação.

## Operação de escrita de uma fonte remota {: #a-remote-sources-write-operation }

Uma fonte OpenAPI, GraphQL remota ou gRPC oferece operações de escrita. Registrar uma como command a torna chamável, governada e auditada a partir de toda superfície. Adicionar a fonte não registra nenhuma; você registra as que quer, uma a uma, como registra tabelas. O registro é a curadoria.

O que uma fonte oferece:

| Tipo de fonte | Operações oferecidas | Nome da operação |
| --- | --- | --- |
| `openapi` | Toda operação não-GET da especificação | O `operationId` |
| `graphql_remote` | Todo campo do tipo `Mutation` remoto | O nome do campo |
| `grpc_remote` | Todo método classificado como mutação | `Service.Method` |

### Registrar uma {: #register-one }

1. Abra **Modelo → Comandos** e adicione um command.
2. Escolha a fonte remota. O formulário muda para um seletor de operações, que lista o que a fonte oferece.
3. Escolha a operação, um domínio e os papéis que podem chamá-la.
4. Opcionalmente, ative **Requer aprovação** e preencha o campo **Escreve na tabela**.

Pela API GraphQL de administração, `availableFunctions(sourceId, schemaName)` lista as operações. O nome do esquema é `openapi`, `graphql` ou `grpc_remote`, conforme o tipo de fonte.

Todo o resto decorre da operação, seja o que for que o formulário envie. O Provisa sobrescreve estes campos:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Todo argumento é tipado como `json`. Os argumentos da operação são seus parâmetros de caminho do OpenAPI (mais `body`, quando a operação recebe um corpo de requisição), seus argumentos de mutação do GraphQL ou seus campos de requisição do gRPC. Uma operação que a fonte não oferece é recusada com 422, `functions.operation_not_offered`.

### Chamando {: #calling-it }

O Provisa não molda, não tipa nem verifica a entrada. Cada argumento vai ao serviço remoto sem alteração, com a credencial da fonte, e a resposta do serviço remoto volta sem alteração. O Provisa governa quem pode chamar, em qual domínio, se é preciso aprovação, e registra a chamada.

Como o serviço remoto recebe os argumentos:

- **OpenAPI.** Os parâmetros de caminho preenchem o modelo do caminho. `body` é o corpo JSON da requisição. Todo outro argumento vai na query string.
- **GraphQL.** Os argumentos são enviados como variáveis tipadas, cada uma declarada com o tipo que o esquema remoto lhe dá. A mutação pede de volta os campos escalares e de enum da resposta, e os dos objetos dentro dela, até dois níveis de profundidade.
- **gRPC.** Os argumentos tornam-se a mensagem de requisição de `Service.Method`.

A resposta são as linhas do command: um objeto é uma linha, uma lista de objetos são suas linhas, qualquer outra coisa é uma linha `{"result": ...}`.

No GraphQL, o command é um campo de mutação e sua resposta é o escalar JSON.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

Nas superfícies SQL (pgwire e as demais que repassam SQL), escreva cada argumento como um literal JSON dentro de uma string. `'{"title": "x"}'` é um objeto, `'"text"'` uma string, `'3'` um número. Um literal que não é JSON válido falha com 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

No REST, envie por POST um objeto JSON de argumentos para `/data/rest/{domain}/commands/{command}`. Um argumento `json` é documentado na especificação gerada como qualquer valor.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Recusas {: #refusals }

A recusa do serviço remoto volta como o serviço a formulou.

| Serviço remoto | O Provisa responde |
| --- | --- |
| Recusa a chamada (HTTP 4xx, `errors` do GraphQL, uma chamada gRPC recusada) | 422, `functions.remote_refused`, com `remote_status` e `answer` |
| Falha (HTTP 5xx) | 502, `functions.remote_refused` |

Cabe ao serviço remoto dizer, quando a operação é chamada, se a credencial da fonte pode executá-la. O Provisa não consegue testar uma escrita no registro sem executá-la.

### Aprovação {: #approval }

Ative **Requer aprovação** e cada chamada é submetida ao hook de aprovação da implantação antes de executar. Ela só executa se o hook aprovar.

- Nenhum hook configurado: 403, `functions.approval_unavailable`.
- O hook nega: 403, `functions.approval_denied`, com o motivo do hook.

O hook recebe o chamador, o papel, o nome do command e seus argumentos. Veja [Hook de Aprovação ABAC](security.md#abac-approval-hook). A flag é armazenada como `Function.requires_approval`, e a verificação vale para qualquer command que a defina.

### Escreve na tabela {: #writes-table }

Informe no campo **Escreve na tabela** a tabela em que a operação escreve, como `schema.table`. Ela precisa ser uma tabela registrada da própria fonte do command, ou o salvamento é recusado com 422, `actions.written_table_not_registered`. A configuração é opcional.

Após cada chamada que o serviço remoto aceita, o Provisa a trata como uma escrita nessa tabela. Ele descarta as respostas em cache da tabela, marca como obsoletas as views materializadas sobre ela, emite o evento de mudança, executa os comandos que rodam quando ela muda e recarrega a tabela quando ela é mantida hot.

Quando a tabela é lida de sua réplica completa (as configurações do operador a colocam ali, ou o motor não consegue ler sua fonte no local), após a chamada é pedida uma construção da réplica, com o motivo `write`. Os leitores mantêm a réplica antiga até que a nova a substitua. Uma tabela replicada linha a linha, ou com uma coluna de parâmetro, não tem uma réplica completa a reconstruir.

### Por que não pode ser composta {: #why-it-cannot-be-composed }

Uma operação de escrita é uma ação. Ela não transforma dados, e uma view ou view materializada que contivesse uma executaria a escrita a cada leitura ou atualização. Por isso a chamada fica sozinha. `SELECT * FROM create_issue(...)` sozinho a executa, e a mesma chamada dentro de uma instrução maior é recusada onde quer que esteja, num join, numa subconsulta ou numa projeção.

Uma view ou view materializada cuja definição chama uma é recusada ao ser salva. Uma definição que não pode ser analisada também é recusada enquanto houver alguma operação de escrita registrada, pois não se pode demonstrar que ela não chama nenhuma.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

Uma operação de fonte também não é um nó de linhagem. A linhagem é lida do SQL de views e consultas, onde um comando aparece como nó, e nenhuma definição salva pode chamar uma operação de fonte.

## Commands e lineage {: #commands-and-lineage }

Como todo command declara suas colunas de entrada e saída, o lineage em nível de coluna **fecha através
da fronteira opaca do command**. O motor de lineage aplica um fechamento de contaminação: cada coluna de saída
declarada deriva de cada coluna de entrada declarada.

**A consequência prática:** a largura do seu contrato de entrada define a precisão desse fechamento. Uma entrada estreita, só com as colunas de que o comando precisa, produz um cone de linhagem justo e legível. Declarar cada coluna da relação de origem abre o leque sobre cada saída. Continua correto, porque nenhuma linhagem se perde, mas embaça a rastreabilidade.

**Regra prática:** passe a projeção mínima de que o comando precisa e devolva só colunas derivadas. Entradas devolvidas sem mudança alargam o cone de contaminação, então deixe-as de fora.

Veja [Lineage](lineage.md) para saber como nós de command aparecem no DAG e como lê-los.

## Lista de permissões de egresso {: #egress-allowlist }

Comandos `http` e `grpc` chamam endpoints externos. Todo host de destino precisa constar na `udf_egress_allowlist` do deployment. O loopback (`localhost`, `127.0.0.1`, `::1`) é sempre permitido. Sem allowlist, toda saída externa é negada com HTTP 403, e não há padrão silencioso.

## Código de host {: #host-code }

Um comando `python` chama um `module:attr` dentro do processo do servidor, e um comando `script` roda um subprocesso no host do servidor. Qualquer um dos dois roda com tudo o que o servidor alcança: sua memória, suas credenciais, seus arquivos e sua rede. Um código malicioso, com defeito ou voraz pode ler qualquer parte disso, esgotar os recursos do servidor ou derrubá-lo. A entrada do comando no modelo guarda só uma referência ao código (`module:attr`, ou o caminho do script), então o código em si não viaja com o modelo quando o modelo é promovido para outro ambiente.

A configuração de implantação `commands.allow_host_code` rege os dois tipos. Ela vem desligada por padrão. Enquanto estiver desligada:

- registrar um comando de qualquer um dos dois tipos é recusado, seja pela API de administração, seja por um arquivo de configuração;
- atualizar um comando para qualquer um dos dois tipos é recusado;
- chamar um comando existente de qualquer um dos dois tipos é recusado, com HTTP 403 e o código
  `functions.host_code_refused`.

A configuração pertence ao operador, nunca a uma organização. Ela fica na página Configurações de implantação, em Segurança, e também pode ser definida no arquivo de configuração ou no ambiente:

```yaml
server:
  commands:
    allow_host_code: true
```

```bash
PROVISA_COMMANDS_ALLOW_HOST_CODE=true
```

As [fontes de código](code-sources.md) são a alternativa governada: seu código roda em um sandbox, é fixado em uma versão e é promovido com o modelo.

## Rastreamento de invocação {: #invocation-tracing }

Toda invocação emite um trace, seja qual for o resultado. O trace contém o nome do comando, o tipo de transporte, o modelo de identidade (DEFINER ou INVOKER), as referências às relações de entrada, o id do papel e a cardinalidade de saída. Quem o emite é o dispatcher, então nenhum `impl_kind` consegue pulá-lo.

## CLI: provisa metadata export

`provisa metadata export` é um job do nível de shell e roda fora do caminho de RPC governado. Ele dispara a publicação de metadados sob demanda do servidor em execução enviando para `/admin/metadata-export/publish`, o endpoint que o botão **Publish now** da aba Admin chama.

Use-o para conduzir exportações agendadas a partir de cron ou CI quando o agendamento `reconcile_cron` configurado não
for granular o bastante:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Saída 0 = publicação completa. Saída 1 = publicação parcial ou falha de conexão.

Para a referência completa de flags, opções de autenticação, nomenclatura de host em multilocação e um exemplo de cron, veja
[Exportação de metadados — pela linha de comando](metadata-export.md#from-the-command-line).

Commands aparecem na projeção git de cada ambiente. Veja [Ambientes](environments.md) para saber como um command e suas atribuições de tag sobrevivem a merge e pull.
