# Commands

Um command é uma função registrada e governada que traz computação externa para dentro do sistema de
governança, auditoria e lineage do Provisa. Onde o motor de federação trata SQL nativamente, um command
é a costura para a computação que ele não consegue expressar: um microsserviço de enriquecimento, um modelo Python, um script
de shell, um procedimento armazenado nativo do banco de dados. Registre-o uma vez; toda interface de cliente — GraphQL,
SQL pgwire, REST, Arrow Flight, gRPC, Bolt/Cypher — pode invocá-lo com governança idêntica
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

A distinção-chave: um command é um **RPC governado**, não um ETL ad hoc. Suas entradas e saídas são
declaradas, tipadas, validadas, rastreadas e ligadas ao lineage. Uma chamada curl ou um subprocesso não governado
não é nada disso.

## Tipos de implementação

Seis valores de `impl_kind` são suportados [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]:

| `impl_kind` | Transporte |
| --- | --- |
| `source_procedure` | Procedimento armazenado nativo em uma fonte registrada |
| `source_operation` | Uma operação de escrita de uma fonte OpenAPI, GraphQL remota ou gRPC, repassada como está (veja [Operação de escrita de uma fonte remota](#operacao-de-escrita-de-uma-fonte-remota-req-1924)) |
| `script` | Subprocesso local alimentado com JSON no stdin, lê JSON do stdout |
| `http` | Endpoint HTTP/S; corpo de requisição JSON, resposta JSON |
| `grpc` | gRPC unário; ponte JSON sem proto |
| `python` | Callable Python em processo (`module:attr`) |

O endereçamento (o `name` no catálogo e o `function_name`) é desacoplado do `binding` (transporte e
localização). Troque o binding e a governança, o lineage e os contratos com quem chama o command permanecem
inalterados. [tool-verified: Function model in models.py:710-750]

## Tipos de argumento

Cada argumento declara um `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Comportamento |
| --- | --- |
| `column_value` | Escalar; passado diretamente no payload da requisição |
| `table_ref` | Preguiçoso; o Provisa passa a referência da relação como está; o serviço busca os dados |
| `result_set` | Ansioso; o Provisa materializa a relação referenciada e envia suas linhas |

Commands `http` e `grpc` **devem** declarar ao menos um argumento `table_ref` ou `result_set`.
Um command externo recebendo apenas argumentos escalares seria invocado uma vez por linha, o que derrota
o batching. O dispatcher rejeita esta configuração no momento da chamada (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

Um command que retorna um conjunto (declarado via `output_columns` e `return_schema`) é uma
função com valor de tabela. Use-o em uma cláusula `FROM` ou em um `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## O contrato de conjunto de dados (REQ-1159)

Cada argumento `table_ref` ou `result_set` pode declarar um **contrato de colunas de entrada**: uma lista ordenada
e tipada em IR de colunas em `FunctionArgument.columns`. O próprio command declara um
**contrato de colunas de saída** em `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Ambos os contratos são validados de forma fail-loud em toda invocação:

- **Entrada (somente result_set):** após a materialização, o Provisa valida as linhas contra as
  colunas declaradas. Campos extras, campos faltantes e tipos errados levantam todos HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Saída:** as linhas retornadas pelo command são validadas contra `output_columns` antes de
  chegarem a quem chamou. [tool-verified: function_dispatch.py:488-490]
- **Projeção estreita:** quando um contrato de entrada é declarado, a consulta de materialização projeta
  **somente essas colunas** (`SELECT "id", "region" FROM ...`) em vez de `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### O vocabulário de tipos IR

Os tipos de coluna do contrato usam o sistema de tipos IR canônico (REQ-846), não escalares GraphQL ou
grafias nativas da fonte. Os nomes válidos são [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Aliases comuns resolvem automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` é a **projeção GraphQL** de `output_columns`, não a fonte da verdade.
Declare `output_columns` para validação e lineage; adicione `return_schema` para geração de tipos
GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## Escrevendo um command

### Arquivo de configuração

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

[tool-verified: sample_config.yaml enrich_orders block]

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

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### UI de administração

O formulário de command em **Configurações → Commands** inclui um editor de colunas de entrada por conjunto de dados (uma linha
por coluna declarada, com um seletor de tipo IR) e um editor de colunas de saída. Salve o formulário para
registrar ou atualizar o command sem recarregar a configuração. [inferred from CommandFormFields.tsx]

## Composição inline (REQ-1159)

Commands podem aparecer **dentro** de uma declaração SQL maior — em join, em subconsulta ou projetados. Você
não está limitado a `SELECT * FROM fn(args)`. A exceção é a operação de escrita de uma fonte remota, que é chamada sozinha (veja [Por que não pode ser composta](#por-que-nao-pode-ser-composta)).

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
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

A substituição se adapta ao tamanho: até 1.000 linhas o resultado é embutido como uma lista `VALUES` tipada;
acima desse limite ele é registrado como uma relação local nomeada no motor.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Uma declaração localizada é roteada normalmente. Consultas de fonte única permanecem na fonte; somente consultas
genuinamente entre fontes vão para o motor de federação. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Operação de escrita de uma fonte remota (REQ-1924)

Uma fonte OpenAPI, GraphQL remota ou gRPC oferece operações de escrita. Registrar uma como command a torna chamável, governada e auditada a partir de toda superfície. Adicionar a fonte não registra nenhuma; você registra as que quer, uma a uma, como registra tabelas. O registro é a curadoria. [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

O que uma fonte oferece [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]:

| Tipo de fonte | Operações oferecidas | Nome da operação |
| --- | --- | --- |
| `openapi` | Toda operação não-GET da especificação | O `operationId` |
| `graphql_remote` | Todo campo do tipo `Mutation` remoto | O nome do campo |
| `grpc_remote` | Todo método classificado como mutação | `Service.Method` |

### Registrar uma

1. Abra **Modelo → Comandos** e adicione um command.
2. Escolha a fonte remota. O formulário muda para um seletor de operações, que lista o que a fonte oferece.
3. Escolha a operação, um domínio e os papéis que podem chamá-la.
4. Opcionalmente, ative **Requer aprovação** e preencha o campo **Escreve na tabela**.

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

Pela API GraphQL de administração, `availableFunctions(sourceId, schemaName)` lista as operações. O nome do esquema é `openapi`, `graphql` ou `grpc_remote`, conforme o tipo de fonte. [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

Todo o resto decorre da operação, não do que o formulário envia. `_as_source_operation` sobrescreve estes campos:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

Todo argumento é tipado como `json`. Os argumentos da operação são seus parâmetros de caminho do OpenAPI (mais `body`, quando a operação recebe um corpo de requisição), seus argumentos de mutação do GraphQL ou seus campos de requisição do gRPC. [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] Uma operação que a fonte não oferece é recusada com 422, `functions.operation_not_offered`. [tool-verified: `offered_operation`]

### Chamando

O Provisa não molda, não tipa nem verifica a entrada. Cada argumento vai ao serviço remoto sem alteração, com a credencial da fonte, e a resposta do serviço remoto volta sem alteração. O Provisa governa quem pode chamar, em qual domínio, se é preciso aprovação, e registra a chamada. [tool-verified: `source_operation.py` module docstring]

Como o serviço remoto recebe os argumentos:

- **OpenAPI.** Os parâmetros de caminho preenchem o modelo do caminho. `body` é o corpo JSON da requisição. Todo outro argumento vai na query string. [tool-verified: `_call_openapi`]
- **GraphQL.** Os argumentos são enviados como variáveis tipadas, cada uma declarada com o tipo que o esquema remoto lhe dá. A mutação pede de volta os campos escalares e de enum da resposta, e os dos objetos dentro dela, até dois níveis de profundidade. [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** Os argumentos tornam-se a mensagem de requisição de `Service.Method`. [tool-verified: `_call_grpc`]

A resposta são as linhas do command: um objeto é uma linha, uma lista de objetos são suas linhas, qualquer outra coisa é uma linha `{"result": ...}`. [tool-verified: `_rows`]

No GraphQL, o command é um campo de mutação e sua resposta é o escalar JSON. [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

Nas superfícies SQL (pgwire e as demais que repassam SQL), escreva cada argumento como um literal JSON dentro de uma string. `'{"title": "x"}'` é um objeto, `'"text"'` uma string, `'3'` um número. Um literal que não é JSON válido falha com 422, `functions.json_argument_invalid`. [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

No REST, envie por POST um objeto JSON de argumentos para `/data/rest/{domain}/commands/{command}`. Um argumento `json` é documentado na especificação gerada como qualquer valor. [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### Recusas

A recusa do serviço remoto volta como o serviço a formulou. [tool-verified: `_refused` in `source_operation.py`]

| Serviço remoto | O Provisa responde |
| --- | --- |
| Recusa a chamada (HTTP 4xx, `errors` do GraphQL, uma chamada gRPC recusada) | 422, `functions.remote_refused`, com `remote_status` e `answer` |
| Falha (HTTP 5xx) | 502, `functions.remote_refused` |

Cabe ao serviço remoto dizer, quando a operação é chamada, se a credencial da fonte pode executá-la. O Provisa não consegue testar uma escrita no registro sem executá-la. [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### Aprovação

Ative **Requer aprovação** e cada chamada é submetida ao hook de aprovação da implantação antes de executar. Ela só executa se o hook aprovar. [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- Nenhum hook configurado: 403, `functions.approval_unavailable`.
- O hook nega: 403, `functions.approval_denied`, com o motivo do hook.

O hook recebe o chamador, o papel, o nome do command e seus argumentos. Veja [Hook de Aprovação ABAC](security.md#hook-de-aprovacao-abac). A flag é armazenada como `Function.requires_approval`, e a verificação vale para qualquer command que a defina. [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### Escreve na tabela

Informe no campo **Escreve na tabela** a tabela em que a operação escreve, como `schema.table`. Ela precisa ser uma tabela registrada da própria fonte do command, ou o salvamento é recusado com 422, `actions.written_table_not_registered`. A configuração é opcional. [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

Após cada chamada que o serviço remoto aceita, o Provisa a trata como uma escrita nessa tabela. Ele descarta as respostas em cache da tabela, marca como obsoletas as views materializadas sobre ela, emite o evento de mudança, executa os sinks da tabela e recarrega a tabela quando ela é mantida hot. [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

Quando a tabela é lida de sua réplica completa (as configurações do operador a colocam ali, ou o motor não consegue ler sua fonte no local), após a chamada é pedida uma construção da réplica, com o motivo `write`. Os leitores mantêm a réplica antiga até que a nova a substitua. Uma tabela replicada linha a linha, ou com uma coluna de parâmetro, não tem uma réplica completa a reconstruir. [tool-verified: `provisa/api/data/table_written.py` `_request_replica_build`; `provisa/federation/replica_state.py` `REASON_WRITE`]

### Por que não pode ser composta

Uma operação de escrita é uma ação, não uma transformação de dados. Uma view ou view materializada que contivesse uma executaria a escrita a cada leitura ou atualização. Por isso a chamada fica sozinha: `SELECT * FROM create_issue(...)` isolada a executa, e a mesma chamada dentro de uma instrução maior é recusada, onde quer que esteja -- em um join, uma subconsulta ou uma projeção. [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

Uma view ou view materializada cuja definição chama uma é recusada ao ser salva. Uma definição que não pode ser analisada também é recusada enquanto houver alguma operação de escrita registrada, pois não se pode demonstrar que ela não chama nenhuma. [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

Uma operação de fonte também não é um nó de lineage: o lineage é lido do SQL de views e consultas, onde um command aparece como nó, e nenhuma definição salva pode chamar uma operação de fonte. [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## Commands e lineage

Como todo command declara suas colunas de entrada e saída, o lineage em nível de coluna **fecha através
da fronteira opaca do command**. O motor de lineage aplica um fechamento de contaminação: cada coluna de saída
declarada deriva de cada coluna de entrada declarada. [tool-verified: `_splice_commands` in graph.py:223-242]

**A consequência prática:** a largura do seu contrato de entrada determina a precisão desse
fechamento. Uma entrada estreita — somente as colunas de que o command realmente precisa — produz um cone de lineage
apertado e legível. Declarar toda coluna da relação de fonte se abre amplamente sobre cada
saída, o que ainda é correto (nenhum lineage é perdido) mas borra a rastreabilidade.

**Regra prática:** passe a projeção mínima de que o command precisa e retorne somente colunas derivadas
(não entradas ecoadas inalteradas). Isso mantém o cone de contaminação preciso. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

Veja [Lineage](lineage.md) para saber como nós de command aparecem no DAG e como lê-los.

## Lista de permissões de egresso

Commands `http` e `grpc` chamam endpoints externos. Todo host de destino deve constar na
`udf_egress_allowlist` da implantação. Loopback (`localhost`, `127.0.0.1`, `::1`) é sempre
permitido. Uma lista de permissões ausente nega todo egresso externo com HTTP 403 — não há padrão
silencioso. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Rastreamento de invocação (REQ-886)

Toda invocação emite um trace independentemente do resultado. O trace inclui o nome do command,
o tipo de transporte, o modelo de identidade (DEFINER ou INVOKER), as referências de relação de entrada, o id da função e
a cardinalidade de saída. O dispatcher emite o trace — nenhum `impl_kind` pode contorná-lo.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` é um job de camada shell, não um RPC governado. Ele dispara a publicação
de metadados sob demanda do servidor em execução (REQ-1072/REQ-1074) postando em
`/admin/metadata-export/publish` — o mesmo endpoint que o botão **Publish now** da aba de administração
chama. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Use-o para conduzir exportações agendadas a partir de cron ou CI quando o agendamento `reconcile_cron` configurado não
for granular o bastante:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Saída 0 = publicação completa. Saída 1 = publicação parcial ou falha de conexão.

Para a referência completa de flags, opções de autenticação, nomenclatura de host em multilocação e um exemplo de cron, veja
[Exportação de metadados — pela linha de comando](metadata-export.md#from-the-command-line).


Commands aparecem na projeção git de cada ambiente. Veja [Ambientes](environments.md) para saber como um command e suas atribuições de tag sobrevivem a merge e pull.
