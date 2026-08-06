# Commands

Um command é uma função registrada e governada que traz computação externa sob o sistema de
governança, auditoria, e lineage do Provisa. Onde o motor de federação lida com SQL nativamente, um
command é a costura para computação que ele não consegue expressar: um microsserviço de
enriquecimento, um modelo Python, um script shell, um stored procedure nativo de banco de dados.
Registre-o uma vez; toda superfície de cliente — GraphQL, SQL pgwire, REST, Arrow Flight, gRPC,
Bolt/Cypher — consegue invocá-lo com governança idêntica (REQ-885, REQ-1156). [tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

A distinção chave: um command é um **RPC governado**, não ETL ad-hoc. Suas entradas e saídas são
declaradas, tipadas, validadas, rastreadas, e conectadas ao lineage. Uma chamada curl ou subprocess
não governada não é nenhuma dessas coisas.

## Tipos de implementação

Cinco valores de `impl_kind` são suportados [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]:

| `impl_kind` | Transporte |
| --- | --- |
| `source_procedure` | Stored procedure nativo em uma fonte registrada |
| `script` | Subprocesso local alimentado com JSON no stdin, lê JSON do stdout |
| `http` | Endpoint HTTP/S; corpo de requisição JSON, resposta JSON |
| `grpc` | gRPC unário; ponte JSON sem proto |
| `python` | Callable Python em processo (`module:attr`) |

O endereçamento (o `name` do catálogo e `function_name`) é desacoplado do `binding` (transporte e
localização). Troque o binding e os contratos de governança, lineage, e chamador do command
permanecem inalterados. [tool-verified: Function model in models.py:710-750]

## Tipos de argumento

Cada argumento declara um `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Comportamento |
| --- | --- |
| `column_value` | Escalar; passado diretamente no payload da requisição |
| `table_ref` | Preguiçoso (lazy); o Provisa passa a referência de relação como está; o serviço busca os dados |
| `result_set` | Ansioso (eager); o Provisa materializa a relação referenciada e envia suas linhas |

Commands `http` e `grpc` **devem** declarar pelo menos um argumento `table_ref` ou `result_set`.
Um command externo recebendo apenas argumentos escalares seria invocado uma vez por linha, o que
anula o batching. O dispatcher rejeita essa configuração no momento da chamada (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

Um command que retorna um conjunto (declarado via `output_columns` e `return_schema`) é uma
função com valor de tabela. Use-o em uma cláusula `FROM` ou um `JOIN`. [inferred from models.py:744-748
and command_localize.py:52-63]

## O contrato de dataset (REQ-1159)

Cada argumento `table_ref` ou `result_set` pode declarar um **contrato de coluna de entrada**: uma
lista ordenada e tipada em IR de colunas em `FunctionArgument.columns`. O próprio command declara um
**contrato de coluna de saída** em `Function.output_columns`. [tool-verified: DatasetColumn model in
models.py:675-683, Function.output_columns in models.py:748]

Ambos os contratos são validados de forma fail-loud em cada invocação:

- **Entrada (somente result_set):** após a materialização, o Provisa valida as linhas contra as
  colunas declaradas. Campos extras, campos faltantes, e tipos errados todos lançam HTTP 422.
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **Saída:** linhas retornadas pelo command são validadas contra `output_columns` antes de
  alcançarem o chamador. [tool-verified: function_dispatch.py:488-490]
- **Projeção estreita:** quando um contrato de entrada é declarado, a consulta de materialização
  projeta **somente essas colunas** (`SELECT "id", "region" FROM ...`) em vez de `SELECT *`.
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### O vocabulário de tipo IR

Tipos de coluna de contrato usam o sistema de tipo IR canônico (REQ-846), não escalares GraphQL ou
grafias nativas de fonte. Os nomes válidos são [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Aliases comuns resolvem automaticamente (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
etc.). [tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` é a **projeção GraphQL** de `output_columns`, não a fonte da verdade.
Declare `output_columns` para validação e lineage; adicione `return_schema` para geração de tipo
GraphQL. [tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## Criando um command

### Arquivo de config

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
e um `binding` com chaves `target` e `method` em vez de `callable`:

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

### UI de Administração

O formulário de command em **Settings → Commands** inclui um editor de colunas de entrada por
dataset (uma linha por coluna declarada, com um seletor de tipo IR) e um editor de colunas de
saída. Salve o formulário para registrar ou atualizar o command sem um recarregamento de config.
[inferred from CommandFormFields.tsx]

## Composição inline (REQ-1159)

Commands podem aparecer **dentro** de uma declaração SQL maior — unidos, em sub-consulta, ou
projetados. Você não está limitado a `SELECT * FROM fn(args)`.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Antes de governança, validação, ou roteamento rodarem, o pipeline detecta chamadas de command
registradas, executa cada uma através do executor governado compartilhado (para que o contrato de
E/S e o modelo de identidade se apliquem exatamente como em uma chamada direta), e reescreve o
local da chamada para uma relação local tipada.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

A substituição é adaptativa ao tamanho: até 1.000 linhas o resultado é inlined como uma lista
`VALUES` tipada; acima desse limite ele se registra como uma relação local nomeada no motor.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

Uma declaração localizada roteia normalmente. Consultas de fonte única permanecem na fonte; apenas
consultas genuinamente entre fontes vão ao motor de federação. [tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## Commands e lineage

Como todo command declara suas colunas de entrada e saída, o lineage em nível de coluna **se fecha
através da fronteira opaca do command**. O motor de lineage aplica um fechamento de contaminação
(taint closure): cada coluna de saída declarada deriva de toda coluna de entrada declarada. [tool-verified: `_splice_commands` in graph.py:223-242]

**A consequência acionável:** a largura do seu contrato de entrada determina a precisão desse
fechamento. Uma entrada estreita — somente as colunas que o command realmente precisa — produz um
cone de lineage estreito e legível. Declarar toda coluna na relação de fonte expande amplamente
através de cada saída, o que ainda é correto (nenhum lineage é perdido) mas turva a rastreabilidade.

**Regra prática:** passe a projeção mínima que o command precisa, e retorne apenas colunas
derivadas (não entradas ecoadas sem alteração). Isso mantém o cone de contaminação preciso. [inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

Veja [Lineage](lineage.md) para como nós de command aparecem no DAG e como lê-los.

## Allowlist de egress

Commands `http` e `grpc` chamam endpoints externos. Todo host alvo deve aparecer na
`udf_egress_allowlist` da implantação. Loopback (`localhost`, `127.0.0.1`, `::1`) é sempre
permitido. Uma allowlist ausente nega todo egress externo com HTTP 403 — não há padrão silencioso.
[tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Rastreamento de invocação (REQ-886)

Toda invocação emite um trace independentemente do resultado. O trace inclui o nome do command,
o tipo de transporte, o modelo de identidade (DEFINER ou INVOKER), referências de relação de
entrada, id de função, e cardinalidade de saída. O dispatcher emite o trace — nenhum `impl_kind`
consegue contorná-lo.
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` é um job de nível shell, não uma RPC governada. Ele dispara a publicação
de metadados sob demanda do servidor em execução (REQ-1072/REQ-1074) fazendo POST em
`/admin/metadata-export/publish` — o mesmo endpoint que o botão **Publicar agora** da aba de
administração chama. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Use-o para conduzir exportações agendadas a partir do cron ou da CI quando o agendamento
configurado em `reconcile_cron` não for granular o suficiente:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Saída 0 = publicação completa. Saída 1 = publicação parcial ou falha de conexão.

Para a referência completa de flags, as opções de autenticação, a nomeação de hosts em
multitenancy e um exemplo de cron, veja [Exportação de metadados — Pela linha de comando](metadata-export.md#from-the-command-line).
