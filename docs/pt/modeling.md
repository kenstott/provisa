<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modelagem de Dados (Entidades e Fatos) {: #data-modeling-entities-facts }

O Provisa oferece duas primitivas declarativas, `entity` e `fact`, que cobrem os blocos de que todo esquema estrela e todo Data Vault são feitos. Declare a especificação, e o Provisa a converte exatamente nas definições de views materializadas, bitemporais e de relacionamentos que você escreveria à mão.

## O que são entidades e fatos {: #what-entities-and-facts-are }

Uma **entity** é uma projeção com chave, deduplicada, opcionalmente historizada de uma relação de
fonte. Você a nomeia, a aponta para uma fonte, declara a chave da entidade e os atributos que quer
carregar, e escolhe um modo de histórico. O Provisa escreve o SQL da view e registra a MV. Quando o
histórico é habilitado, a MV é bitemporal.

Um **fato** é um join com chaves de entidade, reduzido a um grão declarado, com medidas agregadas. O Provisa escreve uma consulta de MV agregada (`GROUP BY` grão + colunas FK) e registra um relacionamento para cada vínculo de dimensão declarado. Um fato sem medidas é um conjunto puro de chaves, o padrão link do Data Vault.

As duas construções são IR. As definições geradas passam de um motor a outro sem remodelar: materializadas no Oracle ou no Databricks, ou deixadas virtuais sobre um motor MPP.

## Modos de histórico {: #history-modes }

Três modos estão disponíveis em uma entity:

| Modo | Significado | Modo bitemporal |
| --- | --- | --- |
| `none` | Somente atual. Sem histórico. | — |
| `scd2` | Rastreia toda mudança. Anexa somente linhas alteradas (delta) chaveadas na chave da entidade. | `delta` |
| `snapshot` | Rastreia toda atualização. Anexa o conjunto de resultado completo a cada atualização, carimbado com tempo de sistema. | `snapshot` |

`scd2` precisa de uma chave de entidade para computar o delta. `snapshot` funciona em qualquer
motor mas o armazenamento cresce por uma cópia completa por atualização. Escolha `scd2` para fontes
grandes e de mudança lenta; escolha `snapshot` quando você precisa de histórico completo e a fonte
não consegue fornecer uma chave.

Fatos não têm modo de histórico próprio; a cobertura deles no tempo vem do histórico das entidades por baixo.

## Medidas e agregações {: #measures-and-aggregations }

Medidas são declaradas como pares `column:agg`. Agregações suportadas:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

A agregação padrão é `sum`.

## Exemplo trabalhado: Entity Customer + Fact Sales {: #worked-example-customer-entity-sales-fact }

### As tabelas de fonte {: #the-source-tables }

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Registre a entity Customer {: #register-the-customer-entity }

=== "UI de Administração"

    1. Abra **Tables** e clique em **+ Model**.
    2. Escolha **Entity (dimension)**.
    3. Preencha o formulário:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(seu domínio)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Clique em **Create**.

=== "API GraphQL"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

O Provisa gera e registra esta MV bitemporal:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Registre o fact Sales {: #register-the-sales-fact }

=== "UI de Administração"

    1. Clique em **+ Model** novamente.
    2. Escolha **Fact**.
    3. Preencha o formulário:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(seu domínio)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Clique em **Create**.

=== "API GraphQL"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

O Provisa gera e registra:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Mais um relacionamento registrado: `Sales.customer_id → Customer` (cardinalidade: many-to-one).

## O formulário Model (UI de administração) {: #the-model-form-admin-ui }

O botão **+ Model** aparece na página **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)").

Um controle segmentado no topo do modal alterna entre **Entity (dimension)** e **Fact**.

### Campos de Entity {: #entity-fields }

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| Name | sim | O nome da MV no catálogo |
| Source relation | sim | Relação com ponto, ex.: `raw.customers` |
| Domain | sim | Domínio ao qual a MV pertence |
| Entity key | sim | Coluna(s) de chave separadas por vírgula, ex.: `id` |
| Attributes | não | Colunas de atributo separadas por vírgula, ex.: `name, region, tier` |
| History | não | `none` / `scd2` / `snapshot`; padrão é `none` |

### Campos de Fact {: #fact-fields }

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| Name | sim | O nome da MV no catálogo |
| Source relation | sim | Relação com ponto, ex.: `raw.orders` |
| Domain | sim | Domínio ao qual a MV pertence |
| Grain | sim | Coluna(s) de grão separadas por vírgula, ex.: `order_id` |
| Measures | não | Pares `col:agg` separados por vírgula, ex.: `amount:sum, quantity:sum` |
| Dimensions | não | Pares `Entity:fk_column` separados por vírgula, ex.: `Customer:customer_id` |

Quando `agg` é omitido em uma measure (`amount` em vez de `amount:sum`), o padrão é `sum`.

## A API GraphQL {: #the-graphql-api }

Ambas as mutações vivem no esquema de administração.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campos de `EntityInput`:

| Campo | Tipo | Padrão | Descrição |
| --- | --- | --- | --- |
| `name` | String | — | Nome de catálogo para a MV de entity |
| `source` | String | — | Relação de fonte (`schema.table` ou entre aspas) |
| `domainId` | String | — | Id do domínio |
| `key` | [String] | — | Coluna(s) de chave da entity |
| `attributes` | [String] | `[]` | Colunas de atributo a projetar |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidade de função |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Campos de `FactInput`:

| Campo | Tipo | Padrão | Descrição |
| --- | --- | --- | --- |
| `name` | String | — | Nome de catálogo para a MV de fact |
| `source` | String | — | Relação de fonte |
| `domainId` | String | — | Id do domínio |
| `grain` | [String] | — | Coluna(s) de grão para o GROUP BY |
| `measures` | [MeasureInput] | `[]` | Pares `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Pares `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidade de função |

`MeasureInput`: `{ column: String, agg: String }`; agg tem `"sum"` como padrão.

`DimRefInput`: `{ entity: String, via: String }`; `entity` é o nome da entity referenciada; `via` é a coluna FK na fonte do fact.

Em caso de sucesso, `registerFact` retorna uma mensagem no formato:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Star schema Kimball e Data Vault {: #kimball-star-schema-and-data-vault }

Nenhum padrão exige ferramentas separadas. As mesmas duas primitivas compõem ambos.

### Star schema Kimball {: #kimball-star-schema }

Este walkthrough constrói um star de três dimensões. Duas tabelas de fonte são novas:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` também ganha colunas `product_id` e `order_date` aqui.

#### Escolhendo o tipo SCD {: #choosing-scd-type }

O modo de histórico é o único controle entre SCD Type 1 e Type 2:

| Tipo SCD | Modo de histórico | Efeito |
| --- | --- | --- |
| Type 1 (somente atual) | `none` | MV reconstruída na atualização; sem histórico de linha |
| Type 2 (versionado) | `scd2` | MV delta bitemporal; cada mudança anexa uma nova linha chaveada na chave da entity |

Use `scd2` quando consultas posteriores precisam unir uma dimensão como ela era no momento da transação, como o nível de um cliente na hora da compra em vez do nível atual. Use `none` para tabelas de consulta estáveis. Um eixo de datas nunca muda, e um catálogo de produtos em que só importa o preço atual pode ser reconstruído a cada atualização.

#### Decisão de grão {: #grain-decision }

O grão é o menor nível de detalhe que o fato responde. `order_id` dá uma linha por pedido e preserva a possibilidade de contar pedidos distintos e unir a qualquer dimensão no nível do pedido. Um grão mais grosso, digamos `["customer_id", "order_date"]`, pré-agrega entre pedidos e descarta esse detalhe para sempre. Declare o grão mais fino de que o negócio precisa; agregações mais grossas são baratas de derivar depois.

#### Registre as dimensões {: #register-the-dimensions }

**Customer** (SCD Type 2 — mudanças de tier devem ser preservadas):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Gera uma MV delta bitemporal chaveada em `id`:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — catálogo atual, nenhum histórico de versão necessário):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

Gera uma MV comum reconstruída na atualização:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (sem histórico — uma data é imutável):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

Gera:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Registre o fact Sales através de três dimensões {: #register-the-sales-fact-across-three-dimensions }

Grão: `order_id`. Três referências de dimensão, com uma coluna FK cada. As duas medidas são somas aditivas.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

O Provisa computa `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` e gera
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Três relacionamentos são registrados automaticamente:

| Relacionamento | Cardinalidade |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensões conformadas {: #conformed-dimensions }

Uma dimensão conformada é registrada uma vez e referenciada por nome por qualquer número de facts.
Suponha que `raw.returns` contenha `return_id`, `customer_id`, `product_id`, e `amount`. O fact
Returns reutiliza Customer e Product sem registrá-los novamente:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

Tanto `Sales` quanto `Returns` apontam para as mesmas entities `Customer` e `Product`. Os caminhos
de join do Provisa garantem que consultas através de qualquer fact percorram a mesma definição de
dimensão
.

---

### Data Vault

As mesmas primitivas mapeiam diretamente para o vocabulário do Data Vault:

| Artefato DV | Primitiva | Histórico |
| --- | --- | --- |
| Hub | `entity` | `none` — somente chaves de entity |
| Satellite | `entity` | `scd2` ou `snapshot` — histórico de atributo ao lado da chave do hub |
| Link | `fact` sem medidas | — |
| Bridge / aggregate link | `fact` com medidas | — |

O exemplo constrói um vault mínimo sobre `raw.customers` e `raw.orders`.

#### Hubs

Um hub contém a chave da entity e nada mais. `attributes: []` com `history: "none"` produz um
conjunto de chave atual deduplicado; o histórico de atributo vive inteiramente no satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Gera:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Gera:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

O satellite fica ao lado da chave do hub e carrega histórico de atributo completo. Use `scd2` para
anexar somente linhas alteradas; use `snapshot` para carimbar cada atualização completa.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Gera:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` e `CustomerHub` ambos chaveiam em `id`. O hub é o alvo de join estável; o satellite
fornece acesso de atributo point-in-time através da camada bitemporal.

#### Link (fact sem medidas) {: #link-measureless-fact }

Um link registra quais chaves de hub apareceram juntas. Ele só guarda chaves, sem medidas, e o Provisa omite o `GROUP BY` quando `measures` está vazio.

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
Sem medidas, então sem `GROUP BY`. Gera:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Dois relacionamentos registrados: `OrderCustomerLink.customer_id → CustomerHub` e
`OrderCustomerLink.order_id → OrderHub`, ambos many-to-one
.

#### Bridge / link agregado {: #bridge-aggregate-link }

Adicione medidas ao link e o Provisa emite o `GROUP BY`, produzindo um bridge pré-agregado. No grão
`order_id` com um cliente por pedido, o resultado é uma linha agregada por pedido:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(o `order_id` duplicado da lista de dimensão é removido por `_dedup`). Gera
:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

O modelo não decide a metodologia. Grão, conformidade, escolha de SCD, e a divisão hub/satellite
permanecem decisões do modelador. O Provisa as executa.

## Metrics

Uma **métrica** é uma definição de agregado com nome e governada, sem grão próprio. O grão, isto é, as dimensões pelas quais o agregado é detalhado, é fixado por quem consulta na hora da consulta e não na definição. É isso que distingue uma métrica de uma view: uma view fixa o grão ao ser criada, e uma métrica fica em aberto até ser consultada.

### O objeto Metric {: #the-metric-object }

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| `name` | sim | snake_case, ex.: `net_revenue`. Validado: `[a-z][a-z0-9_]*` |
| `expression` | sim | ANSI-SQL agregado; deve incluir pelo menos uma função de agregação |
| `datatype` | não | Dica de tipo de resultado, ex.: `number`, `integer` |
| `description` | não | Definição de negócio legível por humano |
| `ai_context` | não | Texto para consumidores de IA — projeta para ferramentas MCP, pg_description, docs GraphQL, e exportação Ossie |
| `visible_to` | não | Lista de função; padrão é `["*"]` (todas as funções) |
| `from_fact` | — | Definido automaticamente quando o metric foi gerado de uma measure de fact |

Referências a colunas dentro da expressão precisam ser qualificadas com a tabela (`orders.amount`, e não só `amount`). Uma coluna sem qualificação é um erro grave quando a métrica é expandida, e nada a deixa passar com um aviso.

O repositório de metric valida a expressão em toda escrita. Uma expressão que não analisa ou não
contém nenhuma função de agregação é rejeitada; nunca é armazenada.

Exemplo de entrada de config:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Consultando um metric {: #querying-a-metric }

O compilador reserva o esquema `metrics`, e toda métrica pode ser endereçada como uma relação virtual dentro dele. Consulte-a como uma tabela, e as colunas que você selecionar viram o conjunto de dimensões e o GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

O compilador reescreve isso em um agregado agrupado real sobre as tabelas semânticas subjacentes
antes de a governança rodar, para que RLS e mascaramento se apliquem às colunas reais.

`SELECT *` contra uma relação de métrica é recusado. Nomeie explicitamente as colunas de dimensão e `value`.

Quando a expressão de um metric abrange múltiplas tabelas, o compilador as une através de
relacionamentos registrados. Uma dimensão que é uma coluna de uma tabela diretamente referenciada
resolve para aquela tabela. Uma dimensão a um hop de relacionamento de distância é unida
automaticamente. Dois hops ou uma dimensão ambígua é um erro rígido nomeando o infrator.

### Metrics de especificações de fact {: #metrics-from-fact-specs }

Quando você registra um fact, cada measure declarada auto-registra um objeto Metric
correspondente. O campo `from_fact` do metric registra o nome da tabela de fact de fonte, e as
dimensões de agrupamento válidas são os atributos de entity alcançáveis sobre os relacionamentos
FK do fact.

Metrics auto-registrados aparecem na página Metrics com um badge **fact**. Você pode editá-los
como qualquer outro metric.

### Views compostas por metric (view_metrics) {: #metric-composed-views-view_metrics }

Uma view `view_metrics` fecha o grão de um metric no momento da definição. Declare os nomes dos
metrics, colunas de dimensão, e filtros opcionais; o compilador gera o SELECT.

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

O compilador gera (para este exemplo):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` e `view_sql` são mutuamente exclusivos na mesma tabela.

**Auto-regeneração na mudança de metric.** Quando a expressão de um metric é atualizada, toda view
`view_metrics` que o referencia recompila e o novo SQL é persistido imediatamente. A view não pode
divergir da definição do metric por construção.

**Chamadas `metric()` inline em SQL de view livre.** `view_sql` escrito manualmente também pode
referenciar metrics via `metric('name')`. O compilador substitui cada chamada pela expressão do
metric e registra uma aresta de lineage. Isso dá a views escritas manualmente a mesma propriedade
de recompilação na mudança quando elas referenciam um metric em vez de copiar sua fórmula.

Observação: views do arquivo de configuração que usam chamadas `metric()` inline são regeneradas quando a configuração é recarregada, e um upsert da métrica não as regenera.

### A página de administração de Metrics {: #the-metrics-admin-page }

Abra o item de navegação **Metrics** para gerenciar metrics governados. Clique em uma linha para
expandir um painel de detalhe somente leitura; clique em **Edit** dentro dele para alternar para
edição inline (sem modal). **New Metric** abre um card de criação inline acima da tabela. A
confirmação de exclusão é o único modal na página.

O formulário de criação/edição oferece um builder de três seletores para metrics originados de
fact: escolha a tabela de fact de fonte (filtrada para `modelingRole=fact`), uma coluna de measure,
e uma função de agregação (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). O datatype é derivado
automaticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type` (o tipo da coluna de measure).
A área de texto de expressão permanece a válvula de escape para expressões arbitrárias.

## O retorno do IR {: #the-ir-payoff }

Toda chamada de registro segue o mesmo caminho de uma MV escrita à mão. A especificação de entity e fact é uma representação intermediária; não é um template nem uma macro. O warehouse de destino é uma propriedade do deployment, e o modelo não o nomeia. Troque o motor de destino, e as mesmas declarações `entity` / `fact` se materializam lá, porque o SQL gerado e os modos bitemporais são neutros quanto ao motor por concepção.
