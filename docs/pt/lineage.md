# Lineage em Nível de Coluna

O Provisa rastreia lineage de dados em nível de coluna de forma estática — computado a partir de definições
SQL e contratos de command, sem execução necessária. Duas views estão disponíveis: um DAG por
declaração e um grafo de proveniência federation-wide abrangendo todas as views e views materializadas
(MVs) registradas.

## O explorador de lineage

Navegue até **Lineage** na UI (`/lineage`). Cole uma declaração SQL e clique em **Build statement
graph** para ver seu DAG em nível de coluna. Clique em **Federation graph** para carregar o grafo de
proveniência sobre todas as MVs no registro. [tool-verified: LineagePage.tsx:28-119]

## DAG em nível de declaração (REQ-1160)

Cada coluna de saída nomeada no seu SQL se torna um nó. O builder a rastreia de volta através de
todo CTE, subconsulta, join, e chamada de command inline até suas colunas de fonte, construindo um
grafo direcionado das entradas de fonte até as saídas finais.

### Exemplo trabalhado

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

- `orders.id`, `orders.region`, e `orders.geo` são nós de **fonte** (o contrato de entrada estreito
  de `enrich_grpc_set` declara `id` e `region`; o fechamento de contaminação completo conecta todas
  as entradas declaradas a todas as saídas). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` e `e.geo` são nós de **command** — a fronteira `enrich_grpc_set`.
- `geo_u` é um nó **derivado** produzido pela função SQL `UPPER`.

A fronteira do command **não é opaca**. Como `enrich_grpc_set` declara suas colunas de entrada
(`id`, `region`) e colunas de saída (`id`, `embedding`, `geo`), o motor de lineage emenda o
fechamento de contaminação continuamente das colunas declaradas da relação de fonte até cada saída.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Tipos de nó e pistas visuais

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Tipo de nó | Cor | Significado |
| --- | --- | --- |
| `source` | Verde | Uma coluna de tabela base |
| `derived` | Azul | Produzida por uma expressão SQL (função, operador, CTE) |
| `command` | Roxo | Uma coluna de saída de um command registrado |

Anéis adicionais em um nó:

- **Anel laranja** — uma coluna de saída final da declaração.
- **Borda dupla** — a relação da coluna é uma view materializada (snapshot MV/CTAS).
- **Anel vermelho** — membro de um ciclo classificado como erro.
- **Anel amarelo** — membro de um ciclo classificado como loop de feedback.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Transformações nomeadas em arestas

Toda aresta carrega a expressão SQL bruta que produz a coluna alvo, mais uma lista de operações
nomeadas: funções SQL (`sql_function`), operadores aritméticos/lógicos (`operator`), commands
registrados (`command`), referências de coluna simples (`identity`), e literais (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

Uma aresta de uma chamada de command é renderizada como uma linha roxa tracejada na UI.
[tool-verified: LineageDag.tsx:122-124]

## Grafo federation-wide (REQ-1161)

O grafo de federação mescla o lineage por declaração de toda MV registrada em um único grafo de
proveniência. A identidade do nó é `relation.column` — a coluna de saída de uma view e a
referência de entrada de outra view para a mesma coluna colapsam em um nó. O resultado é um único
DAG das colunas de fonte base até todo dataset derivado na plataforma. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Use `focus`, `direction`, e `depth` para delimitar a view em escala de federação sem recomputar
o grafo. [tool-verified: `slice_graph` in merge.py:160-189]

## Ciclos (REQ-1161)

Ciclos são descritos, não rejeitados. O motor de lineage detecta todo ciclo direcionado e o
**classifica**. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classificação | Cor da borda | Significado |
| --- | --- | --- |
| `feedback` | Amarelo | O ciclo atravessa um nó materializado — um loop de feedback legal e defasado no tempo. O snapshot da MV é a fronteira de versão que o torna bem definido. |
| `error` | Vermelho | Nenhuma fronteira de materialização no loop — uma definição circular sem ordem de avaliação estável. Provavelmente um erro de design. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Um ciclo `feedback` não é uma falha. Uma MV de enriquecimento que realimenta uma coluna derivada de
volta à sua própria relação de fonte é um padrão válido desde que um nó no loop seja materializado
— o snapshot isola as duas metades temporalmente. Um ciclo `error` precisa de julgamento do
operador: geralmente significa que duas views se referenciam mutuamente sem um snapshot entre elas.

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

Retorna o grafo de proveniência mesclado sobre todas as MVs no registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Parâmetros de consulta [tool-verified: function signature at lineage_router.py:73-76]:

| Parâmetro | Valores | Padrão | Efeito |
| --- | --- | --- | --- |
| `focus` | Um id de nó | — | Delimita a resposta ao subgrafo ao redor deste nó |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Qual direção percorrer a partir de `focus` |
| `depth` | inteiro | ilimitado | Distância máxima de hop a partir de `focus` |

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

## Usando lineage para governar contratos de command

Como o fechamento de contaminação conecta toda coluna de entrada declarada a toda coluna de saída
declarada, a amplitude desse fechamento depende inteiramente do que você declara.

Considere um command que recebe uma tabela orders completa (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) e retorna um `embedding`. Se o contrato de entrada lista
todas essas colunas, toda coluna a jusante que usa o embedding mostrará lineage de todas elas.
Isso é correto mas não útil — é difícil dizer o que realmente importou.

Declare somente `id` e `text` (as colunas que o modelo de embedding realmente lê), e o cone de
lineage se estreita para essas duas colunas de fonte. A derivação é tanto correta quanto precisa.

Veja [Commands](commands.md) para a mecânica de declarar um contrato de entrada estreito.
