# Lineage em Nível de Coluna {: #column-level-lineage }

O Provisa rastreia a linhagem de dados no nível de coluna de forma estática. Ele a calcula a partir das definições SQL e dos contratos de comandos, então nada precisa rodar. Você tem duas visões: um DAG por instrução e um grafo de proveniência de toda a federação, que abrange cada view e view materializada (MV) registrada.

## O explorador de lineage {: #the-lineage-explorer }

Abra **Lineage** na UI (`/lineage`). Cole uma declaração SQL e clique em **Build statement
graph** para ver seu DAG em nível de coluna. Clique em **Federation graph** para carregar o grafo de
proveniência sobre todas as MVs no registro.

## DAG em nível de declaração {: #statement-level-dag }

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
  as entradas declaradas a todas as saídas).
- `e.embedding` e `e.geo` são nós de **command** na fronteira `enrich_grpc_set`.
- `geo_u` é um nó **derivado** produzido pela função SQL `UPPER`.

A fronteira do command **não é opaca**. Como `enrich_grpc_set` declara suas colunas de entrada
(`id`, `region`) e colunas de saída (`id`, `embedding`, `geo`), o motor de lineage emenda o
fechamento de contaminação continuamente das colunas declaradas da relação de fonte até cada saída.

### Tipos de nó e pistas visuais {: #node-kinds-and-visual-cues }

| Tipo de nó | Cor | Significado |
| --- | --- | --- |
| `source` | Verde | Uma coluna de tabela base |
| `derived` | Azul | Produzida por uma expressão SQL (função, operador, CTE) |
| `command` | Roxo | Uma coluna de saída de um command registrado |
| `native` | Laranja | O campo bruto que uma fonte lê, abaixo da coluna de uma tabela registrada |

Anéis adicionais em um nó:

- **Anel laranja** — uma coluna de saída final da declaração.
- **Borda dupla** — a relação da coluna é uma exibição materializada (snapshot MV/CTAS).
- **Anel vermelho** — membro de um ciclo classificado como erro.
- **Anel amarelo** — membro de um ciclo classificado como laço de realimentação.

### Transformações nomeadas nas arestas {: #named-transforms-on-edges }

Toda aresta carrega a expressão SQL bruta que produz a coluna de destino, mais uma lista de
operações nomeadas: funções SQL (`sql_function`), operadores aritméticos/lógicos (`operator`),
commands registrados (`command`), referências simples de coluna (`identity`) e literais (`constant`).

Uma aresta vinda de uma chamada de command é renderizada como uma linha roxa tracejada na UI.

### Abaixo da tabela registrada

Um usuário com registro de fontes vê mais um salto abaixo de cada coluna de origem: o campo como a fonte o lê, numa caixa para o lugar de onde é lido. A caixa traz o logotipo e o rótulo de tipo da fonte, como a página Sources os mostra, e o endereço conforme o tipo de fonte:

| Fonte | Endereço |
| --- | --- |
| OpenAPI | A operação (`GET /v2/customers`), os seus parâmetros fixos e a localização das linhas |
| Remote GraphQL | O campo raiz, os seus argumentos obrigatórios e a localização das linhas |
| MongoDB, Cosmos DB | O banco de dados e a coleção ou o contêiner |
| Elasticsearch, Qdrant | O índice ou a coleção |
| CSV, Parquet, arquivos, logs | O caminho, e o padrão de arquivos de uma tabela que lê muitos arquivos |
| Kafka | O tópico |
| Fonte de código | A função |
| Qualquer outra | O schema e a tabela nativos |

O campo bruto é o caminho de campo da coluna quando ela tem um, senão o próprio nome dela. A aresta dele para a coluna registrada nomeia a renomeação: `path`, `alias` ou `naming` (a convenção de nomes). A caixa da tabela registrada diz como ela é servida: `virtual`, lida a cada instrução, ou `replica`, com o TTL dela e quando carregou pela última vez. Credenciais, referências a segredos e o usuário, a senha e a query de uma URL nunca são mostrados. Um usuário sem registro de fontes vê a linhagem a partir da tabela registrada.

### Links

Cada elemento abre onde é definido, quando os direitos do usuário abrem aquela página. Um clique num campo abre a tabela, a view ou a fonte dele; um clique duplo numa caixa abre a relação dela; um clique na aresta de um comando abre o comando. Uma função SQL embutida não tem página nem link.

## Grafo abrangendo toda a federação {: #federation-wide-graph }

O grafo da federação junta a linhagem por instrução de cada MV registrada num único grafo de proveniência. A identidade de um nó é `relation.column`, então a coluna de saída de uma view e a referência de entrada de outra view a essa coluna se fundem num nó. O resultado é um único DAG, das colunas das fontes base até cada conjunto de dados derivado da plataforma.

Use `focus`, `direction` e `depth` para delimitar a exibição em escala de federação sem recomputar
o grafo.

## Ciclos {: #cycles }

O motor de linhagem detecta cada ciclo dirigido e o **classifica**. Um ciclo é descrito, nunca rejeitado.

| Classificação | Cor da borda | Significado |
| --- | --- | --- |
| `feedback` | Amarelo | O ciclo cruza um nó materializado — um laço de realimentação legítimo e defasado no tempo. O snapshot da MV é a fronteira de versão que o torna bem definido. |
| `error` | Vermelho | Nenhuma fronteira de materialização no laço — uma definição circular sem ordem de avaliação estável. Provavelmente um erro de projeto. |

Um ciclo `feedback` é um padrão válido. Uma MV de enriquecimento que devolve uma coluna derivada à sua própria relação de origem funciona desde que um nó do laço esteja materializado, porque o snapshot separa as duas metades no tempo. Um ciclo `error` precisa do julgamento de um operador. Em geral significa que duas views se referenciam sem um snapshot no meio.

## API

Os dois endpoints são **estáticos**. Eles leem definições e contratos, nunca dados.

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

Formato da resposta:

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

Para um chamador com registro de fontes, os dois endpoints acrescentam o salto nativo: nós do tipo `native` e um objeto `natives` com chave pela relação de cada caixa nativa (`native:<domain>.<table>`):

```json
"natives": {
  "native:sales.customers": {
    "relation": "sales.customers",
    "source": "crm",
    "type": "openapi",
    "hints": {"brand": "stripe"},
    "address": [
      {"what": "operation", "value": "GET /v2/customers"},
      {"what": "rows", "value": "data.items"}
    ],
    "served": {"mode": "virtual"}
  }
}
```

O `served` de uma réplica é `{"mode": "replica", "cache_ttl": 3600, "last_load": "...", "next_load": "..."}`. Para qualquer outro chamador, `natives` vem vazio.

Retorna HTTP 422 quando o SQL não pode ser analisado.

### GET /admin/lineage/federation

Retorna o grafo de proveniência fundido sobre todas as MVs no registro.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Parâmetros de consulta:

| Parâmetro | Valores | Padrão | Efeito |
| --- | --- | --- | --- |
| `focus` | Um id de nó | — | Delimita a resposta ao subgrafo em torno deste nó |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Qual direção percorrer a partir de `focus` |
| `depth` | inteiro | sem limite | Distância máxima em saltos a partir de `focus` |

A resposta tem o mesmo formato do grafo de declaração, com um campo `cycles` adicionado
:

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

## De que cada atualização foi construída {: #what-each-refresh-was-built-from }

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

## Uma view como estava, e como foi registrada {: #a-view-as-it-stood-and-as-it-was-recorded }

Uma view declarada bitemporal guarda cada versão que já teve: cada atualização acrescenta, e nenhuma versão é reescrita, em qualquer armazenamento. Uma leitura simples responde com o estado atual. Dois cabeçalhos da requisição leem o seu passado:

- `X-Provisa-As-Of` lê a view numa data, ou sobre um intervalo, no seu relógio: o tempo de alteração da própria fonte quando a view declara `bitemporal_source_time`, o tempo de carga do Provisa quando não declara.
- `X-Provisa-As-Recorded` lê a view como o Provisa a tinha registrado num momento. Versões registradas depois ficam de fora, de modo que uma correção registrada mais tarde não muda a reprodução do passado.

Em 30 de setembro e registrada em 1º de outubro dá o valor original; registrada em 5 de outubro dá a correção. O registro de auditoria de cada instrução guarda os dois tempos (`as_of`, `as_recorded`), para que um relatório possa ser reproduzido como foi produzido da primeira vez. [Views com histórico](configuration.md#views-with-history-time-travel) descreve como declarar uma view bitemporal e cada superfície que aceita os dois valores.

## O que uma renomeação ou remoção de coluna quebraria {: #what-a-column-rename-or-drop-would-break }

Uma coluna carrega dois nomes, e cada um é armazenado por um conjunto diferente de artefatos.

O **nome exposto** é o que as interfaces SQL e GraphQL mostram: `table_columns.alias`, recaindo
para o padrão snake_case quando nenhum alias está definido. Exibições, exibições materializadas, expressões de métrica, predicados de RLS,
contratos de DQ, grãos de exibição de métrica e chaves de linha de MV são todos escritos contra aquele nome, então
**renomear um alias os quebra tão certamente quanto excluir a coluna**.

O **nome físico** é `table_columns.column_name`, a identidade que sobrevive à substituição integral de
colunas feita pelo upsert da tabela. Relacionamentos, vínculos de [glossário](glossary.md), atribuições de tag, a coluna
de watermark e presets de coluna guardam este, então eles só quebram quando a coluna é **removida**.

`columnDependents` reporta ambos. Exibições e MVs a jusante vêm de fatiar o grafo de federação no
nome exposto da coluna; os artefatos que o grafo não cobre vêm de uma varredura direta do
registro.

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

A página de tabelas roda a consulta automaticamente quando uma edição pendente muda um alias ou reduz o conjunto de colunas, e lista o que encontra. O aviso é consultivo. Ele nomeia os artefatos afetados, e o administrador decide. Ele nunca bloqueia o salvamento, porque não dá para alcançar todos os consumidores do patrimônio: um dashboard externo ou uma aplicação cliente que consulta a coluna pelo nome está fora do conhecimento do registro. Pelo mesmo motivo, varreduras em texto SQL livre casam a coluna como token identificador sem resolver o escopo, então podem nomear um artefato que acaba não usando a coluna. Num aviso, relatar demais é a direção segura.

## Usando lineage para governar contratos de command {: #using-lineage-to-govern-command-contracts }

Como o fechamento de contaminação conecta cada coluna de entrada declarada a cada coluna de saída declarada,
a amplitude desse fechamento depende inteiramente do que você declara.

Pense num comando que recebe uma tabela de pedidos completa (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) e devolve um `embedding`. Se o contrato de entrada lista todas essas colunas, cada coluna a jusante que usa o embedding mostra linhagem de todas elas. Isso é exato, mas não ajuda, porque você não consegue dizer o que de fato importou.

Declare somente `id` e `text` (as colunas que o modelo de embedding de fato lê), e o cone de
lineage se aperta para essas duas colunas de fonte. A derivação é ao mesmo tempo correta e precisa.

Veja [Commands](commands.md) para a mecânica de declarar um contrato de entrada estreito.
