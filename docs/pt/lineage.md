# Lineage em Nível de Coluna {: #column-level-lineage }

O Provisa rastreia lineage de dados em nível de coluna de forma estática — computado a partir de definições
SQL e contratos de command, sem execução necessária. Duas exibições estão disponíveis: um DAG por
declaração e um grafo de proveniência abrangendo toda a federação, cobrindo todas as exibições e exibições
materializadas (MVs) registradas.

## O explorador de lineage {: #the-lineage-explorer }

Navegue até **Lineage** na UI (`/lineage`). Cole uma declaração SQL e clique em **Build statement
graph** para ver seu DAG em nível de coluna. Clique em **Federation graph** para carregar o grafo de
proveniência sobre todas as MVs no registro. [tool-verified: LineagePage.tsx:28-119]

## DAG em nível de declaração (REQ-1160) {: #statement-level-dag-req-1160 }

Cada coluna de saída nomeada no seu SQL se torna um nó. O builder a rastreia de volta através de
todo CTE, subconsulta, join e chamada de command inline até suas colunas de fonte, construindo um
grafo direcionado das entradas de fonte até as saídas finais.

### Exemplo trabalhado {: #worked-example }

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Esta declaração produz três colunas de saída. O grafo para `geo_u` se parece com:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` e `orders.geo` são nós de **fonte** (o contrato de entrada estreito
  de `enrich_grpc_set` declara `id` e `region`; o fechamento de contaminação completo conecta todas
  as entradas declaradas a todas as saídas). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` e `e.geo` são nós de **command** — a fronteira `enrich_grpc_set`.
- `geo_u` é um nó **derivado** produzido pela função SQL `UPPER`.

A fronteira do command **não é opaca**. Como `enrich_grpc_set` declara suas colunas de entrada
(`id`, `region`) e colunas de saída (`id`, `embedding`, `geo`), o motor de lineage emenda o
fechamento de contaminação continuamente das colunas declaradas da relação de fonte até cada saída.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Tipos de nó e pistas visuais {: #node-kinds-and-visual-cues }

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Tipo de nó | Cor | Significado |
| --- | --- | --- |
| `source` | Verde | Uma coluna de tabela base |
| `derived` | Azul | Produzida por uma expressão SQL (função, operador, CTE) |
| `command` | Roxo | Uma coluna de saída de um command registrado |

Anéis adicionais em um nó:

- **Anel laranja** — uma coluna de saída final da declaração.
- **Borda dupla** — a relação da coluna é uma exibição materializada (snapshot MV/CTAS).
- **Anel vermelho** — membro de um ciclo classificado como erro.
- **Anel amarelo** — membro de um ciclo classificado como laço de realimentação.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformações nomeadas nas arestas {: #named-transforms-on-edges }

Toda aresta carrega a expressão SQL bruta que produz a coluna de destino, mais uma lista de
operações nomeadas: funções SQL (`sql_function`), operadores aritméticos/lógicos (`operator`),
commands registrados (`command`), referências simples de coluna (`identity`) e literais (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

Uma aresta vinda de uma chamada de command é renderizada como uma linha roxa tracejada na UI.
[tool-verified: LineageDag.tsx:122-124]

## Grafo abrangendo toda a federação (REQ-1161) {: #federation-wide-graph-req-1161 }

O grafo de federação funde o lineage por declaração de cada MV registrada em um único grafo de proveniência.
A identidade do nó é `relation.column` — a coluna de saída de uma exibição e a referência de entrada de outra
exibição à mesma coluna colapsam em um só nó. O resultado é um único DAG das colunas de fonte base até
cada conjunto de dados derivado na plataforma. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Use `focus`, `direction` e `depth` para delimitar a exibição em escala de federação sem recomputar
o grafo. [tool-verified: `slice_graph` in merge.py:160-189]

## Ciclos (REQ-1161) {: #cycles-req-1161 }

Ciclos são descritos, não rejeitados. O motor de lineage detecta todo ciclo direcionado e o
**classifica**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classificação | Cor da borda | Significado |
| --- | --- | --- |
| `feedback` | Amarelo | O ciclo cruza um nó materializado — um laço de realimentação legítimo e defasado no tempo. O snapshot da MV é a fronteira de versão que o torna bem definido. |
| `error` | Vermelho | Nenhuma fronteira de materialização no laço — uma definição circular sem ordem de avaliação estável. Provavelmente um erro de projeto. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Um ciclo `feedback` não é uma falha. Uma MV de enriquecimento que realimenta uma coluna derivada em sua
própria relação de fonte é um padrão válido desde que um nó do laço seja materializado — o
snapshot isola as duas metades temporalmente. Um ciclo `error` exige julgamento do operador: normalmente
significa que duas exibições se referenciam mutuamente sem snapshot no meio.

## API

Ambos os endpoints são **estáticos** — eles leem definições e contratos, não dados.

### POST /admin/lineage/graph

Retorna o DAG em nível de coluna para uma única declaração SQL.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

Formato da resposta [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

Retorna HTTP 422 quando o SQL não pode ser analisado.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Retorna o grafo de proveniência fundido sobre todas as MVs no registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Parâmetros de consulta [tool-verified: function signature at lineage_router.py:73-76]:

| Parâmetro | Valores | Padrão | Efeito |
| --- | --- | --- | --- |
| `focus` | Um id de nó | — | Delimita a resposta ao subgrafo em torno deste nó |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Qual direção percorrer a partir de `focus` |
| `depth` | inteiro | sem limite | Distância máxima em saltos a partir de `focus` |

A resposta tem o mesmo formato do grafo de declaração, com um campo `cycles` adicionado
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## De que cada atualização foi construída (REQ-862) {: #what-each-refresh-was-built-from-req-862 }

Cada atualização de uma visão materializada registra de que foi construída: a versão da definição da visão, a versão das suas entradas e como essa versão foi estabelecida, e as colunas de origem e a expressão de cada coluna de saída. A visão operacional `"ops"."mv_refresh_lineage"` retorna uma linha por atualização:

| Coluna | Contém |
|---|---|
| `mv_id` | A visão materializada atualizada |
| `target_table` | A tabela que a atualização gravou |
| `definition_version` | A versão da definição da visão |
| `input_version`, `input_version_kind` | A versão das suas entradas: um snapshot do Iceberg, uma marca d'água, um token de atualidade ou o horário da atualização |
| `trace_id` | O trace da atualização em `"ops"."traces"` |
| `refreshed_at` | Quando rodou |
| `column_derivations` | Cada coluna de saída com suas colunas de origem e sua expressão, em JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

A visão lê os registros de atualização em `"ops"."traces"` e responde em todo motor que os guarda. No Trino são as tabelas Iceberg do catálogo `otel`. No DuckDB o servidor grava os próprios registros de trace como arquivos Parquet na sua pasta de telemetria (`PROVISA_TELEMETRY_DIR`, `~/.provisa/telemetry` por padrão), um arquivo por lote exportado numa pasta por dia. A compactação agendada da telemetria (`otel.compact_cron`) junta os arquivos pequenos de cada dia em arquivos de até 64 MB. Cada organização lê só as próprias atualizações, e os registros são mantidos indefinidamente. Os spans só são registrados enquanto o SDK do OpenTelemetry está habilitado (`OTEL_SDK_DISABLED` não definido ou false).

## Uma view como estava, e como foi registrada (REQ-1163) {: #a-view-as-it-stood-and-as-it-was-recorded-req-1163 }

Uma view declarada bitemporal guarda cada versão que já teve: cada atualização acrescenta, e nenhuma versão é reescrita, em qualquer armazenamento. Uma leitura simples responde com o estado atual. Dois cabeçalhos da requisição leem o seu passado:

- `X-Provisa-As-Of` lê a view numa data, ou sobre um intervalo, no seu relógio: o tempo de alteração da própria fonte quando a view declara `bitemporal_source_time`, o tempo de carga do Provisa quando não declara.
- `X-Provisa-As-Recorded` lê a view como o Provisa a tinha registrado num momento. Versões registradas depois ficam de fora, de modo que uma correção registrada mais tarde não muda a reprodução do passado.

Em 30 de setembro e registrada em 1º de outubro dá o valor original; registrada em 5 de outubro dá a correção. O registro de auditoria de cada instrução guarda os dois tempos (`as_of`, `as_recorded`), para que um relatório possa ser reproduzido como foi produzido da primeira vez. [Views com histórico](configuration.md#views-with-history-time-travel) descreve como declarar uma view bitemporal e cada superfície que aceita os dois valores.

## O que uma renomeação ou remoção de coluna quebraria (REQ-1484) {: #what-a-column-rename-or-drop-would-break-req-1484 }

Uma coluna carrega dois nomes, e cada um é armazenado por um conjunto diferente de artefatos.

O **nome exposto** é o que as interfaces SQL e GraphQL mostram: `table_columns.alias`, recaindo
para o padrão snake_case quando nenhum alias está definido [tool-verified: `computed_sql_alias` at
`schema_helpers.py:317`]. Exibições, exibições materializadas, expressões de métrica, predicados de RLS,
contratos de DQ, grãos de exibição de métrica e chaves de linha de MV são todos escritos contra aquele nome, então
**renomear um alias os quebra tão certamente quanto excluir a coluna**.

O **nome físico** é `table_columns.column_name`, a identidade que sobrevive à substituição integral de
colunas feita pelo upsert da tabela. Relacionamentos, vínculos de [glossário](glossary.md), atribuições de tag, a coluna
de watermark e presets de coluna guardam este, então eles só quebram quando a coluna é **removida**.

`columnDependents` reporta ambos. Exibições e MVs a jusante vêm de fatiar o grafo de federação no
nome exposto da coluna; os artefatos que o grafo não cobre vêm de uma varredura direta do
registro [tool-verified: `graph_dependents` in `provisa/lineage/dependents.py`, registry scans in
`provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` é `rename` para uma referência ao nome exposto e `remove` para uma ao nome físico, de modo que quem
chama consegue dizer a qual metade da edição cada artefato está reagindo.

Pergunte isto **antes** de salvar. Uma coluna renomeada é localizada pelo nome exposto que ela ainda carrega no
registro; uma vez que o alias tenha sido aplicado, o nome antigo se foi e a consulta não encontra nada.

A página de Tabelas roda a consulta automaticamente quando uma edição pendente muda um alias ou encolhe o
conjunto de colunas, e lista o que encontra [tool-verified: `diffEditedColumns` in
`provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`]. O aviso é consultivo:
ele nomeia os artefatos afetados e o administrador decide. Ele não bloqueia o salvamento, porque
os consumidores do patrimônio não podem ser todos alcançados — um painel externo ou uma aplicação cliente que
consulta a coluna pelo nome está além do conhecimento do registro. Pela mesma razão, varreduras sobre
texto SQL livre casam a coluna como um token identificador em vez de resolver escopo, o que pode nomear um
artefato que acaba não usando a coluna. Reportar demais é a direção segura para um aviso.

## Usando lineage para governar contratos de command {: #using-lineage-to-govern-command-contracts }

Como o fechamento de contaminação conecta cada coluna de entrada declarada a cada coluna de saída declarada,
a amplitude desse fechamento depende inteiramente do que você declara.

Considere um command que recebe uma tabela de orders completa (`id`, `region`, `amount`, `customer_id`,
`discount`, `notes`, ...) e retorna um `embedding`. Se o contrato de entrada listar todas essas
colunas, cada coluna a jusante que usar o embedding mostrará lineage a partir de todas elas.
Isso é preciso mas não é útil — fica difícil dizer o que realmente importou.

Declare somente `id` e `text` (as colunas que o modelo de embedding de fato lê), e o cone de
lineage se aperta para essas duas colunas de fonte. A derivação é ao mesmo tempo correta e precisa.

Veja [Commands](commands.md) para a mecânica de declarar um contrato de entrada estreito.
